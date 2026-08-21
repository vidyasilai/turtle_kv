//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/tree/key_query.hpp>
//

#include <turtle_kv/tree/leaf/blocked_leaf_page_loader.hpp>
#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.ipp>
#include <turtle_kv/tree/leaf_page_view.hpp>

#include <turtle_kv/import/env.hpp>

namespace turtle_kv {

namespace {

bool try_full_page_query_first();

bool require_sharded_views();

StatusOr<ValueView> find_key_in_pinned_leaf(llfs::PinnedPage& pinned_leaf,
                                            KeyQuery& query,
                                            usize& item_index_out);

StatusOr<ValueView> find_key_in_leaf_using_sharded_views(llfs::PageId leaf_page_id,
                                                         KeyQuery& query,
                                                         usize& item_index_out);

StatusOr<u32> find_key_lower_bound_index_using_sharded_views(llfs::PageId leaf_page_id,
                                                             KeyQuery& query);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename PageIdT, typename TryPinFullLeafFn, typename LoadFullLeafFn>
StatusOr<ValueView> find_key_in_leaf_impl(const PageIdT& leaf_page_id,
                                          KeyQuery& query,
                                          usize& item_index_out,
                                          TryPinFullLeafFn&& try_pin_full_leaf_fn,
                                          LoadFullLeafFn&& load_full_leaf_fn)
{
  BoolStatus filter_negative = query.reject_page(leaf_page_id);

  if (filter_negative == BoolStatus::kTrue) {
    return {batt::StatusCode::kNotFound};
  }

  if (filter_negative == BoolStatus::kFalse) {
    KeyQuery::metrics().filter_positive_count.add(1);
  }

  StatusOr<ValueView> result = [&]() -> StatusOr<ValueView> {
    if (try_full_page_query_first()) {
      KeyQuery::metrics().try_pin_leaf_count.add(1);

      StatusOr<llfs::PinnedPage> full_leaf_page =
          BATT_FORWARD(try_pin_full_leaf_fn)(leaf_page_id, query);

      if (full_leaf_page.ok()) {
        KeyQuery::metrics().try_pin_leaf_success_count.add(1);
        return find_key_in_pinned_leaf(*full_leaf_page, query, item_index_out);
      }
    }

    KeyQuery::metrics().sharded_view_find_count.add(1);

    StatusOr<ValueView> result =
        find_key_in_leaf_using_sharded_views(leaf_page_id, query, item_index_out);

    if (!require_sharded_views() && result.status() == batt::StatusCode::kUnavailable) {
      BATT_ASSIGN_OK_RESULT(llfs::PinnedPage full_leaf_page,
                            BATT_FORWARD(load_full_leaf_fn)(leaf_page_id, query));

      return find_key_in_pinned_leaf(full_leaf_page, query, item_index_out);
    }

    if (result.ok()) {
      KeyQuery::metrics().sharded_view_find_success_count.add(1);
    }

    return result;
  }();

  if (filter_negative == BoolStatus::kFalse && result.status() == batt::StatusCode::kNotFound) {
    KeyQuery::metrics().filter_false_positive_count.add(1);
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct TryPinCachedPageId {
  auto operator()(const llfs::PageId& leaf_page_id, KeyQuery& query) const
  {
    return query.page_loader->try_pin_cached_page(  //
        leaf_page_id,
        llfs::PageLoadOptions{
            LeafPageView::page_layout_id(),
            llfs::PinPageToJob::kDefault,
            llfs::LruPriority{kLeafLruPriority},
        });
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct TryPinCachedPageIdSlot {
  auto operator()(const llfs::PageIdSlot& leaf_page_id_slot, KeyQuery& query) const
  {
    return leaf_page_id_slot.try_pin_through(  //
        *query.page_loader,
        llfs::PageLoadOptions{
            LeafPageView::page_layout_id(),
            llfs::PinPageToJob::kDefault,
            llfs::LruPriority{kLeafLruPriority},
        });
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct LoadFullLeafPageId {
  auto operator()(const llfs::PageId& leaf_page_id, KeyQuery& query) const
  {
    return query.page_loader->load_page(  //
        leaf_page_id,
        llfs::PageLoadOptions{
            LeafPageView::page_layout_id(),
            llfs::PinPageToJob::kDefault,
            llfs::OkIfNotFound{false},
            llfs::LruPriority{kLeafLruPriority},
        });
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct LoadFullLeafPageIdSlot {
  auto operator()(const llfs::PageIdSlot& leaf_page_id_slot, KeyQuery& query) const
  {
    return leaf_page_id_slot.load_through(  //
        *query.page_loader,
        llfs::PageLoadOptions{
            LeafPageView::page_layout_id(),
            llfs::PinPageToJob::kDefault,
            llfs::OkIfNotFound{false},
            llfs::LruPriority{kLeafLruPriority},
        });
  }
};

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> find_key_in_leaf(llfs::PageId leaf_page_id,
                                     KeyQuery& query,
                                     usize& item_index_out)
{
  return find_key_in_leaf_impl(leaf_page_id,
                               query,
                               item_index_out,
                               TryPinCachedPageId{},
                               LoadFullLeafPageId{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> find_key_in_leaf(const llfs::PageIdSlot& leaf_page_id_slot,
                                     KeyQuery& query,
                                     usize& item_index_out)
{
  return find_key_in_leaf_impl(leaf_page_id_slot,
                               query,
                               item_index_out,
                               TryPinCachedPageIdSlot{},
                               LoadFullLeafPageIdSlot{});
}

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<u32> find_key_lower_bound_index_in_pinned_leaf(llfs::PinnedPage& pinned_page,
                                                        KeyQuery& query)
{
  auto& packed_leaf = *PackedBlockedLeafPage::view_of(pinned_page);

  return {BATT_CHECKED_CAST(u32,
                            std::distance(packed_leaf.items_begin(),  //
                                          packed_leaf.lower_bound(query.key())))};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename PageIdT, typename TryPinFullLeafFn, typename LoadFullLeafFn>
StatusOr<u32> find_key_lower_bound_index_impl(const PageIdT& leaf_page_id,
                                              KeyQuery& query,
                                              TryPinFullLeafFn&& try_pin_full_leaf_fn,
                                              LoadFullLeafFn&& load_full_leaf_fn)
{
  StatusOr<llfs::PinnedPage> full_pinned_leaf =
      BATT_FORWARD(try_pin_full_leaf_fn)(leaf_page_id, query);

  // First option: pin the full leaf in the cache; non-blocking/non-loading.
  //
  if (full_pinned_leaf.ok()) {
    return find_key_lower_bound_index_in_pinned_leaf(*full_pinned_leaf, query);
  }

  // Second option: use sharded views to save I/O.
  //
  StatusOr<u32> result = find_key_lower_bound_index_using_sharded_views(leaf_page_id, query);

  // Third options: load the full page.
  //
  if (!require_sharded_views() && result.status() == batt::StatusCode::kUnavailable) {
    BATT_ASSIGN_OK_RESULT(llfs::PinnedPage full_leaf_page,
                          BATT_FORWARD(load_full_leaf_fn)(leaf_page_id, query));

    return find_key_lower_bound_index_in_pinned_leaf(full_leaf_page, query);
  }

  return result;
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<u32> find_key_lower_bound_index(llfs::PageId leaf_page_id, KeyQuery& query)
{
  return find_key_lower_bound_index_impl(leaf_page_id,
                                         query,
                                         TryPinCachedPageId{},
                                         LoadFullLeafPageId{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<u32> find_key_lower_bound_index(const llfs::PageIdSlot& leaf_page_id, KeyQuery& query)
{
  return find_key_lower_bound_index_impl(leaf_page_id,
                                         query,
                                         TryPinCachedPageIdSlot{},
                                         LoadFullLeafPageIdSlot{});
}

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<u32> find_key_lower_bound_index_using_sharded_views(llfs::PageId leaf_page_id,
                                                             KeyQuery& query)
{
  BlockedLeafPageLoader blocked_loader{*query.page_loader,
                                       *query.page_slice_storage,
                                       llfs::PinPageToJob::kDefault,
                                       query.tree_options->block_size()};

  blocked_loader.set_page(leaf_page_id);
  const PackedBlockedLeafPage* packed_leaf = blocked_loader.leaf();
  BATT_CHECK_NOT_NULLPTR(packed_leaf);

  usize block_i = packed_leaf->find_block_index_containing_key(query.key());
  BATT_ASSIGN_OK_RESULT(const PackedLeafBlock* block, blocked_loader.load_block(block_i));
  BATT_CHECK_NOT_NULLPTR(block);

  const PackedKeyValueSlotPtr* found_item = block->lower_bound(query.key());

  u32 block_start = (*packed_leaf->block_starting_item)[block_i].value();
  u32 pos_in_block = std::distance(block->items_begin(), found_item);
  return block_start + pos_in_block;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool try_full_page_query_first()
{
  static const bool b_ = [] {
    const bool turtlekv_enable_full_page_query =
        getenv_as<bool>("turtlekv_enable_full_page_query").value_or(true);

    LOG(INFO) << BATT_INSPECT(turtlekv_enable_full_page_query);

    return turtlekv_enable_full_page_query;
  }();

  return b_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
[[maybe_unused]] bool require_sharded_views()
{
  static const bool b_ = [] {
    const bool turtlekv_require_sharded_views =
        getenv_as<bool>("turtlekv_require_sharded_views").value_or(false);

    LOG(INFO) << BATT_INSPECT(turtlekv_require_sharded_views);

    return turtlekv_require_sharded_views;
  }();

  return b_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> find_key_in_pinned_leaf(llfs::PinnedPage& pinned_leaf,
                                            KeyQuery& query,
                                            usize& item_index_out)
{
  auto& packed_leaf = *PackedBlockedLeafPage::view_of(pinned_leaf);

  auto block_iter = packed_leaf.find_block_containing_key(query.key());
  const PackedKeyValueSlotPtr* found = block_iter->find_key(query.key());
  if (!found) {
    return {batt::StatusCode::kNotFound};
  }

  query.page_slice_storage->pinned_pages.emplace_back(std::move(pinned_leaf));

  PackedBlockedLeafPage::ItemIterator found_iter{block_iter, found};
  item_index_out = std::distance(packed_leaf.items_begin(), found_iter);

  VLOG(1) << "Found key " << batt::c_str_literal(query.key()) << BATT_INSPECT(item_index_out)
          << " Reading value";

  return get_value(*found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> find_key_in_leaf_using_sharded_views(llfs::PageId leaf_page_id,
                                                         KeyQuery& query,
                                                         usize& item_index_out)
{
  BlockedLeafPageLoader blocked_loader{*query.page_loader,
                                       *query.page_slice_storage,
                                       llfs::PinPageToJob::kDefault,
                                       query.tree_options->block_size()};

  blocked_loader.set_page(leaf_page_id);
  const PackedBlockedLeafPage* packed_leaf = blocked_loader.leaf();
  BATT_CHECK_NOT_NULLPTR(packed_leaf);

  usize block_i = packed_leaf->find_block_index_containing_key(query.key());
  BATT_ASSIGN_OK_RESULT(const PackedLeafBlock* block, blocked_loader.load_block(block_i));
  BATT_CHECK_NOT_NULLPTR(block);

  const PackedKeyValueSlotPtr* found = block->find_key(query.key());
  if (!found) {
    return {batt::StatusCode::kNotFound};
  }

  u32 block_start = (*packed_leaf->block_starting_item)[block_i].value();
  u32 pos_in_block = std::distance(block->items_begin(), found);
  item_index_out = block_start + pos_in_block;

  VLOG(1) << "Found key " << batt::c_str_literal(query.key()) << BATT_INSPECT(item_index_out)
          << " Reading value";

  return get_value(*found);
}

}  // namespace

}  // namespace turtle_kv
