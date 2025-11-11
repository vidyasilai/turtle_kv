#include <turtle_kv/tree/batch_update.hpp>
//

namespace turtle_kv {

using TrimResult = BatchUpdate::TrimResult;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void BatchUpdate::update_edit_size_totals()
{
  this->edit_size_totals.emplace(
      this->context.compute_running_total</*decay_to_items=*/false>(this->result_set));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void BatchUpdate::update_edit_size_totals_decayed(
    const MergeCompactor::ResultSet</*decay_to_items=*/true>& decayed_result_set)
{
  this->edit_size_totals.emplace(
      this->context.compute_running_total</*decay_to_items=*/true>(decayed_result_set));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void BatchUpdate::decay_batch_to_items(
    MergeCompactor::ResultSet</*decay_to_items=*/true>& output_result_set)
{
  const batt::TaskCount max_tasks{this->context.worker_pool.size() + 1};
  std::vector<EditView> decayed_items;

  if (max_tasks == 1) {
    for (const EditView& edit : this->result_set.get()) {
      Optional<ItemView> maybe_item = to_item_view(edit);
      if (maybe_item) {
        decayed_items.emplace_back(EditView::from_item_view(*maybe_item));
      }
    }
  } else {
    const ParallelAlgoDefaults& algo_defaults = parallel_algo_defaults();

    auto actual_edits = result_set.get();
    const auto src_begin = actual_edits.begin();
    const auto src_end = actual_edits.end();

    const batt::WorkSlicePlan plan{batt::WorkSliceParams{
                                       algo_defaults.copy_decayed_items.min_task_size,
                                       max_tasks,
                                   },
                                   src_begin,
                                   src_end};

    BATT_CHECK_GT(plan.n_tasks, 0);

    batt::SmallVec<usize, 64> output_size_per_shard(plan.n_tasks);
    BATT_CHECK_EQ(output_size_per_shard.size(), plan.n_tasks);

    // First count the number of non-decayed items in the output for each shard.
    {
      batt::ScopedWorkContext work_context{this->context.worker_pool};

      BATT_CHECK_OK(batt::slice_work(
          work_context,
          plan,
          /*gen_work_fn=*/
          [&](usize task_index, isize task_offset, isize task_size) {
            return [src_begin, task_index, task_offset, task_size, &output_size_per_shard] {
              BATT_CHECK_LT(task_index, output_size_per_shard.size());

              auto task_src_begin = std::next(src_begin, task_offset);
              const auto task_src_end = std::next(task_src_begin, task_size);

              usize output_size = 0;

              for (; task_src_begin != task_src_end; ++task_src_begin) {
                if (decays_to_item(*task_src_begin)) {
                  output_size += 1;
                }
              }
              output_size_per_shard[task_index] = output_size;
            };
          }))
          << "worker_pool must not be closed!";
    }

    // Change to a rolling sum and do the actual copy.
    //
    usize output_total_size = 0;
    batt::SmallVec<usize, 64> output_shard_offset;
    for (usize output_shard_size : output_size_per_shard) {
      output_shard_offset.emplace_back(output_total_size);
      output_total_size += output_shard_size;
    }

    decayed_items.resize(output_total_size);
    {
      this->context.worker_pool.reset();

      batt::ScopedWorkContext work_context{this->context.worker_pool};

      BATT_CHECK_OK(
          batt::slice_work(work_context,
                           plan,
                           /*gen_work_fn=*/
                           [&](usize task_index, isize task_offset, isize task_size) {
                             return [src_begin,
                                     &output_shard_offset,
                                     &output_size_per_shard,
                                     task_index,
                                     task_offset,
                                     task_size,
                                     &decayed_items] {
                               auto task_src_begin = std::next(src_begin, task_offset);
                               const auto task_src_end = std::next(task_src_begin, task_size);

                               BATT_CHECK_LT(task_index, output_shard_offset.size());
                               auto task_dst_begin =
                                   std::next(decayed_items.data(), output_shard_offset[task_index]);

                               for (; task_src_begin != task_src_end; ++task_src_begin) {
                                 Optional<ItemView> maybe_item = to_item_view(*task_src_begin);
                                 if (maybe_item) {
                                    *task_dst_begin = EditView::from_item_view(*maybe_item);
                                    ++task_dst_begin;
                                  }
                               }
                             };
                           }))
          << "worker_pool must not be closed!";
    }
  }

  output_result_set.append(std::move(decayed_items));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize BatchUpdate::get_byte_size()
{
  if (!this->edit_size_totals) {
    this->update_edit_size_totals();
  }
  return this->edit_size_totals->back() - this->edit_size_totals->front();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TrimResult BatchUpdate::trim_back_down_to_size(usize byte_size_limit)
{
  TrimResult result;

  if (!this->edit_size_totals) {
    this->update_edit_size_totals();
  }

  const usize orig_byte_size = this->edit_size_totals->back() - this->edit_size_totals->front();
  if (orig_byte_size <= byte_size_limit) {
    return result;
  }

  const usize orig_edit_count = this->result_set.size();

  usize new_byte_size = orig_byte_size;
  usize new_edit_count =                                                        //
      std::distance(this->edit_size_totals->begin(),                            //
                    std::lower_bound(this->edit_size_totals->begin(),           //
                                     std::prev(this->edit_size_totals->end()),  //
                                     byte_size_limit));

  BATT_CHECK_LE(new_edit_count, this->result_set.size());

  while (new_edit_count > 0) {
    new_byte_size = (*this->edit_size_totals)[new_edit_count] - this->edit_size_totals->front();
    if (new_byte_size <= byte_size_limit) {
      while (new_edit_count + 1 < this->edit_size_totals->size()) {
        const usize next_edit_size = (*this->edit_size_totals)[new_edit_count + 1] -
                                     (*this->edit_size_totals)[new_edit_count];
        if (new_byte_size + next_edit_size > byte_size_limit) {
          break;
        }
        new_byte_size += next_edit_size;
        new_edit_count += 1;
      }
      break;
    }

    BATT_CHECK_GT(new_edit_count, 0);
    --new_edit_count;
  }

  this->result_set.drop_after_n(new_edit_count);
  this->edit_size_totals->set_size(new_edit_count + 1);

  BATT_CHECK_EQ(this->result_set.size(), new_edit_count);
  BATT_CHECK_EQ(this->edit_size_totals->size(), new_edit_count + 1);
  BATT_CHECK_EQ(this->edit_size_totals->back() - this->edit_size_totals->front(), new_byte_size);
  BATT_CHECK_GE(orig_edit_count, new_edit_count);
  BATT_CHECK_GE(orig_byte_size, new_byte_size);

  result.n_items_trimmed = orig_edit_count - new_edit_count;
  result.n_bytes_trimmed = orig_byte_size - new_byte_size;

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const BatchUpdate::TrimResult& t)
{
  return out << "TrimResult{.n_items_trimmed=" << t.n_items_trimmed
             << ", .n_bytes_trimmed=" << t.n_bytes_trimmed << ",}";
}

}  // namespace turtle_kv
