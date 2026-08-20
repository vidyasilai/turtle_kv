//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <turtle_kv/tree/active_pivots_set.hpp>
#include <turtle_kv/tree/leaf/blocked_leaf_page_loader.concept.hpp>
#include <turtle_kv/tree/leaf/scan_blocked_leaf.hpp>

#include <turtle_kv/core/edit_slice.hpp>
#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/seq.hpp>

#include <boost/range/irange.hpp>

#include <concepts>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept SegmentedLevelNode = requires(const T& node, usize i) {
  { node.pivot_count() } -> std::convertible_to<usize>;
  { node.get_pivot_key(i) } -> std::convertible_to<KeyView>;
};

template <typename T>
concept SegmentedLevel = requires(const T& level, usize i) {
  typename T::Segment;
  { level.segment_count() } -> std::convertible_to<usize>;
  { level.get_segment(i) } -> std::same_as<const typename T::Segment&>;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Scans all live items across segments in a segmented level, yielding EditSlice items.
 *
 * Iterates segments in the level, loads each leaf page via the block loader, and delegates
 * per-segment scanning to scan_blocked_leaf. Segments whose active pivot range falls entirely
 * below min_pivot_i are skipped.
 */
template <SegmentedLevelNode NodeT, SegmentedLevel LevelT, BlockedLeafPageLoader BlockLoaderT>
auto scan_segmented_level(const NodeT& node,
                          const LevelT& level,
                          BlockLoaderT& block_loader,
                          Status& status,
                          i32 min_pivot_i = 0,
                          Optional<KeyView> min_key = None)
{
  namespace seq = batt::seq;

  // Deduce the per-segment inner sequence type returned by scan_blocked_leaf.
  //
  using InnerSeq = decltype(scan_blocked_leaf(
      std::declval<const PackedBlockedLeafPage*>(),
      std::declval<BlockLoaderT*>(),
      std::declval<const typename LevelT::Segment&>().get_filter(std::declval<const LevelT&>()),
      std::declval<Interval<KeyView>>()));

  return batt::as_seq(boost::irange<usize>(0, level.segment_count())) |
         seq::filter_map([&node, &level, &block_loader, &status, min_pivot_i, min_key](
                             usize segment_i) -> Optional<InnerSeq> {
           if (!status.ok()) {
             return None;
           }

           const auto& segment = level.get_segment(segment_i);
           auto active_pivots = segment.get_active_pivots();

           BATT_CHECK(!active_pivots.is_empty()) << "This segment should have been dropped!";

           // Skip segments containing data before the scan's starting pivot.
           //
           if (active_pivots.last() < min_pivot_i) {
             return None;
           }

           // Compute the scan key range for this segment. If the minimum possible starting pivot
           // is inactive, find the next higher pivot that is active.
           //
           i32 first_pivot = std::max(active_pivots.first(), min_pivot_i);
           while (first_pivot < (i32)node.pivot_count() && !active_pivots.get(first_pivot)) {
             ++first_pivot;
           }
           KeyView lower_bound = node.get_pivot_key(first_pivot);
           if (min_key && KeyOrder{}(lower_bound, *min_key)) {
             lower_bound = *min_key;
           }
           KeyView upper_bound = node.get_pivot_key(active_pivots.last() + 1);

           // Load the leaf page for this segment.
           //
           auto leaf = block_loader.set_page(segment.get_leaf_page_id());
           if (!leaf.ok()) {
             status = leaf.status();
             return None;
           }

           return scan_blocked_leaf(*leaf,
                                    &block_loader,
                                    segment.get_filter(level),
                                    Interval<KeyView>{lower_bound, upper_bound});
         }) |
         seq::flatten() | seq::status_ok() |
         seq::map([](Slice<const PackedKeyValueSlotPtr> slice) -> EditSlice {
           return EditSlice{slice};
         });
}

}  // namespace turtle_kv
