#pragma once

#include <turtle_kv/core/algo/compute_running_total.hpp>

#include <turtle_kv/core/merge_compactor.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_loader.hpp>

#include <batteries/algo/running_total.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/worker_pool.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct BatchUpdateContext {
  batt::WorkerPool& worker_pool;
  llfs::PageLoader& page_loader;
  batt::CancelToken cancel_token;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Uses the worker_pool to perform a parallel merge-compaction of the lines
   * produced by the passed `generator_fn`, up to and including (but stopping at) `max_key`.
   */
  template <bool kDecayToItems, typename GeneratorFn>
  StatusOr<MergeCompactor::ResultSet<kDecayToItems>> merge_compact_edits(
      const KeyView& max_key,
      GeneratorFn&& generator_fn);

  /** \brief Computes and returns the running total (prefix sum) of the edit sizes in result_set.
   */
  template <bool kDecayToItems>
  batt::RunningTotal compute_running_total(
      const MergeCompactor::ResultSet<kDecayToItems>& result_set) const
  {
    return ::turtle_kv::compute_running_total<kDecayToItems>(this->worker_pool, result_set);
  }

  /** \brief Returns a `ResultSet` with only the edits from the batch passed into the function
   * that decay to base-level items (e.g., no tombstones).
   */
  MergeCompactor::ResultSet</*decay_to_items=*/true> decay_batch_to_items(
      MergeCompactor::ResultSet</*decay_to_items=*/false>& batch);
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct BatchUpdate {
  struct TrimResult {
    usize n_items_trimmed = 0;
    usize n_bytes_trimmed = 0;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BatchUpdateContext context;
  MergeCompactor::ResultSet</*decay_to_items=*/false> result_set;
  Optional<batt::RunningTotal> edit_size_totals;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Resets `this->edit_size_totals` to reflect `this->result_set`.
   */
  void update_edit_size_totals();

  /** \brief Returns the inclusive (closed) interval of keys in this batch.
   */
  CInterval<KeyView> get_key_crange() const
  {
    BATT_CHECK(!this->result_set.empty());
    return this->result_set.get_key_crange();
  }

  /** \brief Trim items from the end/back of the result_set, such that the total batch size (in
   * bytes) is not greater than `byte_size_limit`.
   */
  TrimResult trim_back_down_to_size(usize byte_size_limit);

  /** \brief Calculates the size of `result_set` if necessary, and returns the total number of bytes
   * in this batch.
   */
  usize get_byte_size();
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const BatchUpdate::TrimResult& t);

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems, typename GeneratorFn>
inline StatusOr<MergeCompactor::ResultSet<kDecayToItems>> BatchUpdateContext::merge_compact_edits(
    const KeyView& max_key,
    GeneratorFn&& generator_fn)
{
  MergeCompactor compactor{this->worker_pool};

  compactor.start_push_levels();
  BATT_REQUIRE_OK(BATT_FORWARD(generator_fn)(compactor));
  compactor.finish_push_levels();

  MergeCompactor::OutputBuffer<kDecayToItems> edit_buffer;

  this->worker_pool.reset();
  return compactor.read(edit_buffer, max_key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline MergeCompactor::ResultSet</*decay_to_items=*/true> BatchUpdateContext::decay_batch_to_items(
    MergeCompactor::ResultSet</*decay_to_items=*/false>& batch)
{
  const batt::TaskCount max_tasks{this->worker_pool.size() + 1};
  std::vector<EditView> decayed_items;

  if (max_tasks == 1) {
    for (const EditView& edit : batch.get()) {
      Optional<ItemView> maybe_item = to_item_view(edit);
      if (maybe_item) {
        decayed_items.emplace_back(EditView::from_item_view(*maybe_item));
      }
    }
  } else {
    const ParallelAlgoDefaults& algo_defaults = parallel_algo_defaults();

    auto actual_edits = batch.get();
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
      batt::ScopedWorkContext work_context{this->worker_pool};

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
      this->worker_pool.reset();

      batt::ScopedWorkContext work_context{this->worker_pool};

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

  MergeCompactor::ResultSet</*decay_to_items=*/true> output_result_set;
  output_result_set.append(std::move(decayed_items));
  return output_result_set;
}

}  // namespace turtle_kv
