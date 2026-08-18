//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <turtle_kv/tree/active_pivots_set.hpp>
#include <turtle_kv/tree/leaf/scan_blocked_leaf.hpp>

#include <turtle_kv/core/edit_slice.hpp>
#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/seq.hpp>

#include <boost/range/irange.hpp>

namespace turtle_kv {

/** \brief Scans all live items across segments in a segmented level, yielding EditSlice items.
 *
 * Returns a composed batt sequence that iterates segments, loads each page via the block loader,
 * and delegates per-segment scanning to scan_blocked_leaf.
 *
 * NodeT concept:
 *   node.pivot_count() -> usize
 *   node.get_pivot_key(usize i) -> KeyView
 *
 * LevelT concept:
 *   typename LevelT::Segment
 *   level.segment_count() -> usize
 *   level.get_segment(usize i) -> const Segment&
 *
 * BlockLoaderT concept:
 *   block_loader.set_page(llfs::PageId) -> StatusOr<const PackedBlockedLeafPage*>
 *   block_loader.load_block(u32 block_index) -> StatusOr<const PackedLeafBlock*>
 */
template <typename NodeT, typename LevelT, typename BlockLoaderT>
auto scan_segmented_level(const NodeT& node,
                          const LevelT& level,
                          BlockLoaderT& block_loader,
                          Status& status,
                          i32 min_pivot_i = 0,
                          Optional<KeyView> min_key = None)
{
  namespace seq = batt::seq;
  using InnerSeq = decltype(scan_blocked_leaf(
      std::declval<const PackedBlockedLeafPage*>(),
      std::declval<BlockLoaderT*>(),
      std::declval<const typename LevelT::Segment&>().get_filter(std::declval<const LevelT&>()),
      std::declval<Interval<KeyView>>()));

  return batt::as_seq(boost::irange<usize>(0, level.segment_count()))
       | seq::filter_map([&node, &level, &block_loader, &status, min_pivot_i, min_key](
                             usize segment_i) -> Optional<InnerSeq> {
           if (!status.ok()) {
             return None;
           }

           const auto& segment = level.get_segment(segment_i);
           auto active_pivots = segment.get_active_pivots();

           BATT_CHECK(!active_pivots.is_empty()) << "This segment should have been dropped!";

           if (active_pivots.last() < min_pivot_i) {
             return None;
           }

           i32 first_pivot = std::max(active_pivots.first(), min_pivot_i);
           while (first_pivot < (i32)node.pivot_count() && !active_pivots.get(first_pivot)) {
             ++first_pivot;
           }
           KeyView lower_bound = node.get_pivot_key(first_pivot);
           if (min_key && KeyOrder{}(lower_bound, *min_key)) {
             lower_bound = *min_key;
           }
           KeyView upper_bound = node.get_pivot_key(active_pivots.last() + 1);

           auto leaf = block_loader.set_page(segment.get_leaf_page_id());
           if (!leaf.ok()) {
             status = leaf.status();
             return None;
           }

           return scan_blocked_leaf(*leaf, &block_loader, segment.get_filter(level),
                                    Interval<KeyView>{lower_bound, upper_bound});
         })
       | seq::flatten()
       | seq::status_ok()
       | seq::map([](Slice<const PackedKeyValueSlotPtr> slice) -> EditSlice {
           return EditSlice{slice};
         });
}

}  // namespace turtle_kv
