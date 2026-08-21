//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.hpp>

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_buffer.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>

#include <memory>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class LeafPageView : public llfs::PageView
{
 public:
  static llfs::PageLayoutId page_layout_id();

  static llfs::PageReader page_reader();

  static Status register_layout(llfs::PageCache& cache);

  static bool layout_used_by_page(const llfs::PinnedPage& pinned_page);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit LeafPageView(std::shared_ptr<const llfs::PageBuffer>&& page_buffer) noexcept;

  llfs::PageLayoutId get_page_layout_id() const override
  {
    return LeafPageView::page_layout_id();
  }

  BoxedSeq<llfs::PageId> trace_refs() const override;

  Optional<KeyView> min_key() const override;

  Optional<KeyView> max_key() const override;

  void dump_to_ostream(std::ostream& out) const override
  {
    out << "LeafPageView";
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const PackedBlockedLeafPage& packed_leaf_page() const
  {
    return *this->packed_leaf_page_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const PackedBlockedLeafPage* packed_leaf_page_;
};

}  // namespace turtle_kv
