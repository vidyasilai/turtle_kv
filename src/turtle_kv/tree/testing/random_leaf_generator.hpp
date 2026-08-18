#pragma once

#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.hpp>
#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.ipp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/testing/fake_page_loader.hpp>
#include <turtle_kv/tree/testing/fake_pinned_page.hpp>

#include <turtle_kv/core/algo/compute_running_total.hpp>
#include <turtle_kv/core/algo/split_parts.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/testing/generate.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_id.hpp>

#include <atomic>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class RandomLeafGenerator
{
 public:
  using Self = RandomLeafGenerator;

  template <bool kDecayToItems>
  struct Result {
    MergeCompactor::ResultSet<kDecayToItems> result_set;
    std::vector<FakePinnedPage> leaf_pages;
    std::vector<llfs::PageId> leaf_page_ids;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static llfs::PageId make_fake_page_id()
  {
    static std::atomic<u64> next_page_id_int{1};
    return llfs::PageId{next_page_id_int.fetch_add(1)};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  RandomResultSetGenerator& items_generator()
  {
    return this->items_generator_;
  }

  Self& set_block_size(usize block_size)
  {
    this->block_size_ = block_size;
    return *this;
  }

  template <bool kDecayToItems, typename Rng>
  Result<kDecayToItems> operator()(DecayToItem<kDecayToItems> decay_to_items,
                                   Rng& rng,
                                   FakePageLoader& fake_loader,
                                   batt::StableStringStore& store)
  {
    Result<kDecayToItems> result;
    llfs::PageSize page_size = fake_loader.get_page_size();

    // Generate a sorted run of random key/value pairs.
    //
    result.result_set = this->items_generator_(decay_to_items, rng, store, /*deleted=*/{});

    batt::WorkerPool& worker_pool = batt::WorkerPool::null_pool();

    const usize flush_size = usize(page_size) * 15 / 16;

    // Compute a running total of packed sizes, so we can split the result set in to leaf pages.
    //
    batt::RunningTotal running_total =
        compute_running_total(worker_pool, result.result_set, DecayToItem<kDecayToItems>{});

    SplitParts page_parts = split_parts(  //
        running_total,                    //
        MinPartSize{flush_size / 4},      //
        MaxPartSize{flush_size},          //
        MaxItemSize{384});

    for (const Interval<usize>& part_extents : page_parts) {
      const auto items_slice = result.result_set.get();
      const auto page_items = batt::slice_range(items_slice, part_extents);

      // We need a fake id for our fake leaf.
      //
      llfs::PageId page_id = Self::make_fake_page_id();

      // Use the fake loader to allocate a page buffer that will be fake-loadable later on.
      //
      FakePinnedPage fake_pinned_page =
          BATT_OK_RESULT_OR_PANIC(fake_loader.load_page(page_id,
                                                        llfs::PageLoadOptions{
                                                            LeafPageView::page_layout_id(),
                                                            llfs::OkIfNotFound{false},
                                                        }));

      // Grab the PageBuffer so we can build the page.
      //
      std::shared_ptr<llfs::PageBuffer> page_buffer = fake_pinned_page.get_page_buffer();

      StatusOr<PackedLeafResult> pack_result =
          pack_blocked_leaf_page(this->block_size_, page_items, page_buffer->mutable_buffer());

      BATT_CHECK(pack_result.ok()) << BATT_INSPECT(pack_result.status());
      BATT_CHECK_EQ(pack_result->items_packed, usize(std::end(page_items) - std::begin(page_items)));

      // Add the fake page and id to the result.
      //
      result.leaf_pages.emplace_back(std::move(fake_pinned_page));
      result.leaf_page_ids.emplace_back(page_id);
    }

    return result;
  }

 private:
  RandomResultSetGenerator items_generator_;
  usize block_size_ = 8 * 1024;
};

}  // namespace testing
}  // namespace turtle_kv
