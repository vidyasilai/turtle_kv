//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/tree/leaf/blocked_leaf_page_view.hpp>
//

#include <llfs/packed_page_header.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ llfs::PageLayoutId BlockedLeafPageView::page_layout_id()
{
  static const llfs::PageLayoutId id = llfs::PageLayoutId::from_str("kv_bleaf");
  return id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ llfs::PageReader BlockedLeafPageView::page_reader()
{
  return [](std::shared_ptr<const llfs::PageBuffer> page_buffer)
             -> StatusOr<std::shared_ptr<const llfs::PageView>> {
    return {std::make_shared<BlockedLeafPageView>(std::move(page_buffer))};
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::Status BlockedLeafPageView::register_layout(llfs::PageCache& cache)
{
  LOG_FIRST_N(INFO, 1) << "Registering page layout: " << BlockedLeafPageView::page_layout_id();
  return cache.register_page_reader(BlockedLeafPageView::page_layout_id(),
                                    __FILE__,
                                    __LINE__,
                                    BlockedLeafPageView::page_reader());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ bool BlockedLeafPageView::layout_used_by_page(const llfs::PinnedPage& pinned_page)
{
  return pinned_page && (llfs::get_page_header(pinned_page.page_buffer()).layout_id ==
                         BlockedLeafPageView::page_layout_id());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ BlockedLeafPageView::BlockedLeafPageView(
    std::shared_ptr<const llfs::PageBuffer>&& page_buffer) noexcept
    : PageView{std::move(page_buffer)}
    , packed_leaf_page_{static_cast<const PackedBlockedLeafPage*>(this->const_payload().data())}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<llfs::PageId> BlockedLeafPageView::trace_refs() const /*override*/
{
  return batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> BlockedLeafPageView::min_key() const /*override*/
{
  if (!this->packed_leaf_page_->item_count()) {
    return None;
  }
  return this->packed_leaf_page_->min_key();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> BlockedLeafPageView::max_key() const /*override*/
{
  if (!this->packed_leaf_page_->item_count()) {
    return None;
  }
  return this->packed_leaf_page_->max_key();
}

}  // namespace turtle_kv
