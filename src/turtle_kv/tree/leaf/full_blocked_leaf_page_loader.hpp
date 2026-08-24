//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include "packed_blocked_leaf_page.hpp"
#include "packed_leaf_block.hpp"

#include <turtle_kv/config.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename PageLoaderT>
class FullBlockedLeafPageLoader
{
 public:
  explicit FullBlockedLeafPageLoader(const PackedBlockedLeafPage* leaf) noexcept
      : leaf_{leaf}
  {
  }

  explicit FullBlockedLeafPageLoader(PageLoaderT& page_loader,
                                     llfs::PinPageToJob pin_page_to_job,
                                     llfs::PageCacheOvercommit& overcommit) noexcept
      : page_loader_{&page_loader}
      , pin_page_to_job_{pin_page_to_job}
      , overcommit_{&overcommit}
  {
  }

  StatusOr<const PackedBlockedLeafPage*> set_page(llfs::PageId page_id) noexcept
  {
    BATT_CHECK_NOT_NULLPTR(this->page_loader_);
    BATT_CHECK_NOT_NULLPTR(this->overcommit_);

    BATT_ASSIGN_OK_RESULT(this->pinned_page_,
                          this->page_loader_->load_page(page_id,
                                                        llfs::PageLoadOptions{
                                                            LeafPageView::page_layout_id(),
                                                            this->pin_page_to_job_,
                                                            llfs::OkIfNotFound{false},
                                                            llfs::LruPriority{kLeafLruPriority},
                                                            *this->overcommit_,
                                                        }));
    this->leaf_ = PackedBlockedLeafPage::view_of(this->pinned_page_);
    return this->leaf_;
  }

  StatusOr<const PackedLeafBlock*> load_block(u32 block_index) noexcept
  {
    BATT_CHECK_NOT_NULLPTR(this->leaf_);
    return &*(this->leaf_->blocks_begin() + block_index);
  }

 private:
  PageLoaderT* page_loader_ = nullptr;
  llfs::PinPageToJob pin_page_to_job_ = llfs::PinPageToJob::kDefault;
  llfs::PageCacheOvercommit* overcommit_ = nullptr;
  typename PageLoaderT::PinnedPageT pinned_page_;
  const PackedBlockedLeafPage* leaf_ = nullptr;
};

}  // namespace turtle_kv
