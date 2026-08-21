#include <turtle_kv/kv_store_scanner.hpp>
//
#include <turtle_kv/tree/leaf/scan_blocked_leaf.hpp>

#include <turtle_kv/util/piecewise_filter.hpp>
#include <turtle_kv/util/piecewise_filter.ipp>

namespace turtle_kv {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <artc::ARTBase::Synchronized kSynchronized>
KeyView art_scanner_get_key(artc::ART<void>::Scanner<kSynchronized>& scanner)
{
  return scanner.get_key();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <artc::ARTBase::Synchronized kSynchronized>
KeyView art_scanner_get_key(artc::ART<MemTableValueEntry>::Scanner<kSynchronized,
                                                                   /*kValuesOnly=*/true>& scanner)
{
  return scanner.get_value().key_view();
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::KVStoreScanner(KVStore& kv_store, const KeyView& min_key) noexcept
    : state_reader_{kv_store.state_}
    , page_loader_{kv_store.per_thread_.get(&kv_store).get_page_loader()}
    , slice_storage_{std::addressof(*(kv_store.per_thread_.get(&kv_store).scan_result_storage))}
    , root_{this->state_reader_->base_checkpoint_->tree()->page_id_slot_or_panic()}
    , tree_height_{this->state_reader_->base_checkpoint_->tree_height()}
    , min_key_{min_key}
    , needs_resume_{false}
    , next_item_{None}
    , status_{OkStatus()}
    , mem_table_value_scanner_{}
    , delta_storage_{this->static_delta_storage_.data()}
    , tree_scan_path_{}
    , scan_levels_{}
    , heap_{}
    , block_loader_{this->page_loader_,
                    *this->slice_storage_,
                    llfs::PinPageToJob::kFalse,
                    kv_store.tree_options().block_size()}
{
  auto& m = KVStoreScanner::metrics();
  m.ctor_count.add(1);

#if TURTLE_KV_PROFILE_QUERIES
  LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.ctor_latency};
#endif

  this->mem_table_value_scanner_.emplace(this->state_reader_->mem_table_->art_index(), min_key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::KVStoreScanner(llfs::PageLoader& page_loader,
                                            const llfs::PageIdSlot& root,
                                            i32 tree_height,
                                            const KeyView& min_key,
                                            PageSliceStorage* slice_storage,
                                            usize block_size) noexcept
    : state_reader_{}
    , page_loader_{page_loader}
    , slice_storage_{slice_storage}
    , root_{root}
    , tree_height_{tree_height}
    , min_key_{min_key}
    , needs_resume_{false}
    , next_item_{None}
    , status_{OkStatus()}
    , delta_storage_{this->static_delta_storage_.data()}
    , tree_scan_path_{}
    , scan_levels_{}
    , heap_{}
    , block_loader_{this->page_loader_,
                    *this->slice_storage_,
                    llfs::PinPageToJob::kFalse,
                    block_size}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KVStoreScanner::~KVStoreScanner() noexcept
{
  if (this->delta_storage_ != this->static_delta_storage_.data()) {
    delete[] this->delta_storage_;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::start()
{
  auto& m = KVStoreScanner::metrics();
  m.start_count.add(1);
#if TURTLE_KV_PROFILE_QUERIES
  LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.start_latency};
#endif

  if (this->state_reader_) {
#if TURTLE_KV_PROFILE_QUERIES
    LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.start_deltas_latency};
#endif

    const usize n_deltas = this->state_reader_->deltas_.size();

    // Reserve space for MemTable (active + deltas) in ScanLevels.
    //
    this->scan_levels_.reserve(1 + n_deltas + kMaxHeapSize);

    // Create the active MemTable scanner.
    //
    BATT_CHECK(this->mem_table_value_scanner_);
    {
      if (!this->mem_table_value_scanner_->is_done()) {
        this->scan_levels_.emplace_back(ActiveMemTableValueTag{}, *this->mem_table_value_scanner_);
      }
    }

    // Reserve space for delta MemTable scanners.
    //
    if (n_deltas > this->static_delta_storage_.size()) {
      this->delta_storage_ = new DeltaMemTableScannerStorage[n_deltas];
    }

    // Create scanners for delta MemTables.
    //
    {
      DeltaMemTableScannerStorage* p_mem = this->delta_storage_;
      for (usize delta_i = n_deltas; delta_i > 0;) {
        --delta_i;

        MemTable& delta_mem_table = *this->state_reader_->deltas_[delta_i];

        // Delta case : single ART index for keys and values
        //
        auto& art_scanner = *(new (
            p_mem) artc::ART<MemTableValueEntry>::Scanner<artc::ARTBase::Synchronized::kFalse,  //
                                                          /*kValuesOnly=*/true>{
            delta_mem_table.art_index(),
            this->min_key_,
        });
        ++p_mem;

        if (!art_scanner.is_done()) {
          this->scan_levels_.emplace_back(DeltaMemTableValueTag{}, art_scanner);
        }
        continue;
      }
    }
  }
  // (ART<void>::Scanner::~Scanner() has no side-effects, so just skip calling destructors)

  // Initialize a path down the checkpoint tree (unless empty).
  //
  if (this->root_.is_valid()) {
    {
#if TURTLE_KV_PROFILE_QUERIES
      LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.start_enter_subtree_latency};
#endif
      BATT_REQUIRE_OK(this->enter_subtree(this->tree_height_, this->root_, std::false_type{}));
    }
    {
#if TURTLE_KV_PROFILE_QUERIES
      LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.start_resume_latency};
#endif
      BATT_REQUIRE_OK(this->resume());
    }
  }

  // Run make heap once at the beginning.
  //
  {
    m.init_heap_size_stats.update(this->scan_levels_.size());
#if TURTLE_KV_PROFILE_QUERIES
    LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.start_build_heap_latency};
#endif
    this->heap_.reset(as_slice(this->scan_levels_), /*minimum_capacity=*/kMaxHeapSize);
  }

  BATT_REQUIRE_OK(this->set_next_item());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto KVStoreScanner::peek() -> const Optional<Item>&
{
  return this->next_item_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto KVStoreScanner::next() -> Optional<Item>
{
  Optional<Item> item;
  std::swap(item, this->next_item_);
  if (item) {
    this->status_.Update(this->set_next_item());
  }
  return item;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::status() const
{
  return this->status_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> KVStoreScanner::read(const Slice<std::pair<KeyView, ValueView>>& buffer)
{
  usize n_read = 0;

  for (; n_read != buffer.size(); ++n_read) {
    if (!this->next_item_) {
      break;
    }

    buffer[n_read].first = this->next_item_->key;
    buffer[n_read].second = this->next_item_->value;

    this->next_item_ = None;
    BATT_REQUIRE_OK(this->set_next_item());
  }

  return n_read;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> KVStoreScanner::read_keys(const Slice<KeyView>& buffer)
{
  usize n_read = 0;

  for (; n_read != buffer.size(); ++n_read) {
    if (!this->next_item_) {
      break;
    }

    buffer[n_read] = this->next_item_->key;

    this->next_item_ = None;
    BATT_REQUIRE_OK(this->set_next_item());
  }

  return n_read;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::validate_page_layout(i32 height, const llfs::PinnedPage& pinned_page)
{
  const auto& page_header =
      *static_cast<const llfs::PackedPageHeader*>(pinned_page->const_buffer().data());

  if (height > 1) {
    if (page_header.layout_id != NodePageView::page_layout_id()) {
      return {batt::StatusCode::kDataLoss};
    }
  } else {
    BATT_CHECK_EQ(height, 1);
    if (page_header.layout_id != LeafPageView::page_layout_id()) {
      return {batt::StatusCode::kDataLoss};
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename InsertHeapBool>
Status KVStoreScanner::enter_subtree(i32 subtree_height,
                                     llfs::PageIdSlot subtree_root,
                                     InsertHeapBool insert_heap)
{
  for (;;) {
    auto load_options = llfs::PageLoadOptions{
        (subtree_height > 1) ? llfs::PinPageToJob::kDefault : llfs::PinPageToJob::kFalse,
        llfs::OkIfNotFound{false},
        llfs::LruPriority{(subtree_height > 1) ? kNodeLruPriority : kLeafLruPriority},
    };

    // Handle the bottom level specially.
    //
    if (subtree_height == 1) {
      metrics().full_leaf_attempts.add(1);
      StatusOr<llfs::PinnedPage> pinned_leaf =
          subtree_root.try_pin_through(this->page_loader_, load_options);

      if (pinned_leaf.ok()) {
        metrics().full_leaf_success.add(1);

        const auto& page_header =
            *static_cast<const llfs::PackedPageHeader*>(pinned_leaf->const_buffer().data());

        if (page_header.layout_id != LeafPageView::page_layout_id()) {
          return {batt::StatusCode::kDataLoss};
        }

        BATT_REQUIRE_OK(this->enter_leaf(std::move(*pinned_leaf), insert_heap));

      } else {
        // If the pin failed, use BlockedLeafPageLoader.
        //
        this->tree_scan_path_.emplace_back(*this, subtree_root.page_id, insert_heap);
      }
      break;
    }

    StatusOr<llfs::PinnedPage> pinned_page =
        subtree_root.load_through(this->page_loader_, load_options);

    BATT_REQUIRE_OK(pinned_page);
    BATT_REQUIRE_OK(this->validate_page_layout(subtree_height, *pinned_page));

    BATT_REQUIRE_OK(this->enter_node(std::move(*pinned_page), insert_heap));

    NodeScanState& node_state = this->tree_scan_path_.back();

    subtree_root = llfs::PageIdSlot::from_page_id(
        node_state.node_->get_child_id(node_state.pivot_i_).unpack());

    --subtree_height;
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename InsertHeapBool>
Status KVStoreScanner::enter_leaf(llfs::PinnedPage&& pinned_page, InsertHeapBool insert_heap)
{
  const PackedBlockedLeafPage& leaf = *PackedBlockedLeafPage::view_of(pinned_page);
  this->tree_scan_path_.emplace_back(*this, std::move(pinned_page), leaf, insert_heap);
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename InsertHeapBool>
Status KVStoreScanner::enter_node(llfs::PinnedPage&& pinned_page, InsertHeapBool insert_heap)
{
  const PackedNodePage& node = PackedNodePage::view_of(pinned_page);
  this->tree_scan_path_.emplace_back(*this, std::move(pinned_page), node, insert_heap);
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::resume()
{
  this->needs_resume_ = false;

  for (;;) {
    if (this->tree_scan_path_.empty()) {
      break;
    }

    NodeScanState& node_state = this->tree_scan_path_.back();

    if (node_state.node_) {
      if (node_state.pivot_i_ < node_state.node_->pivot_count()) {
        ++node_state.pivot_i_;
        if (node_state.pivot_i_ != node_state.node_->pivot_count()) {
          BATT_REQUIRE_OK(
              this->enter_subtree(node_state.get_height() - 1,
                                  llfs::PageIdSlot::from_page_id(
                                      node_state.node_->get_child_id(node_state.pivot_i_).unpack()),
                                  /*insert_heap=*/std::true_type{}));
          continue;
        }
      }
    }

    if (node_state.active_levels_ != 0) {
      break;
    }

    this->tree_scan_path_.pop_back();
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::set_next_item()
{
  auto& m = KVStoreScanner::metrics();
  m.next_count.add(1);
#if TURTLE_KV_PROFILE_QUERIES
  LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.next_latency};
#endif

  for (;;) {
    if (this->heap_.empty()) {
      if (this->next_item_ && this->next_item_->value.is_delete()) {
        this->next_item_ = None;
      }
      return OkStatus();
    }

    ScanLevel* scan_level = this->heap_.first();

    if (!this->next_item_) {
      this->next_item_.emplace(scan_level->item());

    } else if (this->next_item_->key == scan_level->key) {
      // Search for a terminal value for the item and combine it if necessary.
      //
      if (this->next_item_->needs_combine()) {
        this->next_item_->value = combine(this->next_item_->value, scan_level->value());
      }

    } else {
      // We have reached a terminal value for this->next_item_. Now, we have to decide whether
      // we want to return the item to the function's caller OR discard it, because the terminal
      // value represents a deleted item.
      //
      if (this->next_item_->value == ValueView::deleted()) {
        // Discard the deleted item and continue on to the next iteration of the loop, skipping
        // the logic to advance the current scan_level. We do this because we now need to set the
        // first key in the current scan_level to this->next_item_ to examine it next.
        //
        this->next_item_ = None;
        if (this->needs_resume_) {
          BATT_REQUIRE_OK(this->resume());
        }
        continue;
      } else {
        break;
      }
    }

    if (scan_level->advance()) {
#if TURTLE_KV_PROFILE_QUERIES
      LatencyTimer timer{batt::Every2ToTheConst<8>{},
                         KVStoreScanner::metrics().heap_update_latency};
#endif
      this->heap_.update_first();
    } else {
#if TURTLE_KV_PROFILE_QUERIES
      LatencyTimer timer{batt::Every2ToTheConst<8>{},
                         KVStoreScanner::metrics().heap_remove_latency};
#endif
      this->heap_.remove_first();
      this->needs_resume_ = true;
    }
  }

  if (this->needs_resume_) {
    BATT_REQUIRE_OK(this->resume());
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(const KVSlice& kv_slice,
                                                  NodeScanState* node_state,
                                                  i32 buffer_level_i) noexcept
    : key{get_key(kv_slice.front())}
    , state_impl{TreeLevelScanState{
          .kv_slice = kv_slice,
          .node_state = node_state,
          .buffer_level_i = buffer_level_i,
      }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(
    const KVSlice& kv_slice,
    NodeScanState* node_state,
    PackedBlockedLeafPage::BlockIterator block_iter,
    PackedBlockedLeafPage::BlockIterator block_end) noexcept
    : key{get_key(kv_slice.front())}
    , state_impl{FullLeafScanState{
          .kv_slice = kv_slice,
          .node_state = node_state,
          .block_iter = block_iter,
          .block_end = block_end,
      }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(BlockedLeafTag,
                                                  const KVSlice& kv_slice,
                                                  NodeScanState* node_state) noexcept
    : key{get_key(kv_slice.front())}
    , state_impl{BlockedLeafScanState{
          .kv_slice = kv_slice,
          .node_state = node_state,
      }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(
    ActiveMemTableValueTag,
    artc::ART<MemTableValueEntry>::Scanner<artc::ARTBase::Synchronized::kTrue,
                                           /*kValuesOnly=*/true>& art_scanner) noexcept
    : key{art_scanner_get_key(art_scanner)}
    , state_impl{MemTableValueScanState<artc::ARTBase::Synchronized::kTrue>{
          .art_scanner_ = &art_scanner,
      }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(
    DeltaMemTableValueTag,
    artc::ART<MemTableValueEntry>::Scanner<artc::ARTBase::Synchronized::kFalse,
                                           /*kValuesOnly=*/true>& art_scanner) noexcept
    : key{art_scanner_get_key(art_scanner)}
    , state_impl{MemTableValueScanState<artc::ARTBase::Synchronized::kFalse>{
          .art_scanner_ = &art_scanner,
      }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(
    const Slice<const EditView>& edit_view_slice) noexcept
    : key{edit_view_slice.front().key}
    , state_impl{edit_view_slice}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
EditView KVStoreScanner::ScanLevel::item() const
{
  return batt::case_of(
      this->state_impl,
      [](NoneType) -> EditView {
        BATT_PANIC() << "illegal state";
        BATT_UNREACHABLE();
      },
      [](const MemTableValueScanState<artc::ARTBase::Synchronized::kTrue>& state) -> EditView {
        const MemTableValueEntry& entry = state.art_scanner_->get_value();
        return EditView{entry.key_view(), entry.value_view()};
      },
      [](const MemTableValueScanState<artc::ARTBase::Synchronized::kFalse>& state) -> EditView {
        const MemTableValueEntry& entry = state.art_scanner_->get_value();
        return EditView{entry.key_view(), entry.value_view()};
      },
      [](const Slice<const EditView>& state) -> EditView {
        return state.front();
      },
      [this](const TreeLevelScanState& state) -> EditView {
        return EditView{this->key, get_value(state.kv_slice.front())};
      },
      [this](const FullLeafScanState& state) -> EditView {
        return EditView{this->key, get_value(state.kv_slice.front())};
      },
      [this](const BlockedLeafScanState& state) -> EditView {
        return EditView{this->key, get_value(state.kv_slice.front())};
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ValueView KVStoreScanner::ScanLevel::value() const
{
  return batt::case_of(
      this->state_impl,
      [](NoneType) -> ValueView {
        BATT_PANIC() << "illegal state";
        BATT_UNREACHABLE();
      },
      [](const MemTableValueScanState<artc::ARTBase::Synchronized::kTrue>& state) -> ValueView {
        return state.art_scanner_->get_value().value_view();
      },
      [](const MemTableValueScanState<artc::ARTBase::Synchronized::kFalse>& state) -> ValueView {
        return state.art_scanner_->get_value().value_view();
      },
      [](const Slice<const EditView>& state) -> ValueView {
        return state.front().value;
      },
      [](const TreeLevelScanState& state) -> ValueView {
        return get_value(state.kv_slice.front());
      },
      [](const FullLeafScanState& state) -> ValueView {
        return get_value(state.kv_slice.front());
      },
      [](const BlockedLeafScanState& state) -> ValueView {
        return get_value(state.kv_slice.front());
      });
}

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename MemTableScanStateT>
BATT_ALWAYS_INLINE bool scan_level_mem_table_advance_impl(KVStoreScanner::ScanLevel* scan_level,
                                                          MemTableScanStateT& state)
{
  auto& m = KVStoreScanner::metrics();

  m.art_advance_count.add(1);
#if TURTLE_KV_PROFILE_QUERIES
  LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.art_advance_latency};
#endif

  state.art_scanner_->advance();
  if (state.art_scanner_->is_done()) {
    return false;
  }
  scan_level->key = art_scanner_get_key(*state.art_scanner_);
  return true;
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool KVStoreScanner::ScanLevel::advance()
{
  auto& m = KVStoreScanner::metrics();

  m.scan_level_advance_count.add(1);
#if TURTLE_KV_PROFILE_QUERIES
  LatencyTimer timer{batt::Every2ToTheConst<10>{}, m.scan_level_advance_latency};
#endif

  return batt::case_of(
      this->state_impl,
      [](NoneType) -> bool {
        BATT_PANIC() << "illegal state";
        BATT_UNREACHABLE();
      },
      [this](MemTableValueScanState<artc::ARTBase::Synchronized::kTrue>& state) -> bool {
        return scan_level_mem_table_advance_impl(this, state);
      },
      [this](MemTableValueScanState<artc::ARTBase::Synchronized::kFalse>& state) -> bool {
        return scan_level_mem_table_advance_impl(this, state);
      },
      [this](Slice<const EditView>& state) -> bool {
        state.drop_front();
        if (state.empty()) {
          return false;
        }
        this->key = state.front().key;
        return true;
      },
      [this](TreeLevelScanState& state) -> bool {
        state.kv_slice.drop_front();
        if (state.kv_slice.empty()) {
          state.kv_slice = state.node_state->pull_next(state.buffer_level_i);
          if (state.kv_slice.empty()) {
            return false;
          }
        }
        this->key = get_key(state.kv_slice.front());
        return true;
      },
      [this](FullLeafScanState& state) -> bool {
        state.kv_slice.drop_front();
        if (state.kv_slice.empty()) {
          // Advance to the next block.
          //
          ++state.block_iter;
          while (state.block_iter != state.block_end) {
            state.kv_slice = state.block_iter->items_slice();
            if (!state.kv_slice.empty()) {
              this->key = get_key(state.kv_slice.front());
              return true;
            }
            ++state.block_iter;
          }
          state.node_state->active_levels_ = 0;
          return false;
        }
        this->key = get_key(state.kv_slice.front());
        return true;
      },
      [this](BlockedLeafScanState& state) -> bool {
        state.kv_slice.drop_front();
        if (state.kv_slice.empty()) {
          state.kv_slice = state.node_state->pull_next(0);
          if (state.kv_slice.empty()) {
            return false;
          }
        }
        this->key = get_key(state.kv_slice.front());
        return true;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kInsertHeap>
/*explicit*/ KVStoreScanner::NodeScanState::NodeScanState(
    KVStoreScanner& kv_scanner,
    llfs::PinnedPage&& page,
    const PackedNodePage& node,
    std::integral_constant<bool, kInsertHeap>) noexcept
    : active_levels_{0}
    , pinned_page_{std::move(page)}
    , node_{&node}
    , pivot_i_{(i32)in_node(node).find_pivot_containing(kv_scanner.min_key_)}
    , scan_status_{OkStatus()}
{
  const i32 n_levels = this->node_->get_level_count();

  for (i32 buffer_level_i = 0; buffer_level_i < n_levels; ++buffer_level_i) {
    PackedLevel& level = this->levels_.emplace_back(
        this->node_->is_size_tiered()
            ? this->node_->get_tier(buffer_level_i)
            : this->node_->get_level(buffer_level_i));

    this->level_seqs_.emplace_back(
        scan_segmented_level(*this->node_,
                             level,
                             kv_scanner.block_loader_,
                             this->scan_status_,
                             this->pivot_i_,
                             kv_scanner.min_key_) |
        batt::seq::boxed());

    SlotSlice first_slice = this->pull_next(buffer_level_i);
    if (!first_slice.empty()) {
      this->active_levels_ |= (u64{1} << buffer_level_i);
      ScanLevel& level = kv_scanner.scan_levels_.emplace_back(first_slice, this, buffer_level_i);
      if (kInsertHeap) {
#if TURTLE_KV_PROFILE_QUERIES
        LatencyTimer timer{batt::Every2ToTheConst<8>{},
                           KVStoreScanner::metrics().heap_insert_latency};
#endif
        kv_scanner.heap_.insert(&level);
      }
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kInsertHeap>
/*explicit*/ KVStoreScanner::NodeScanState::NodeScanState(
    KVStoreScanner& kv_scanner,
    llfs::PinnedPage&& page,
    const PackedBlockedLeafPage& leaf,
    std::integral_constant<bool, kInsertHeap>) noexcept
    : active_levels_{0}
    , pinned_page_{std::move(page)}
    , node_{nullptr}
    , pivot_i_{0}
    , scan_status_{OkStatus()}
{
  // Find the block containing the min_key and get the first non-empty slice.
  //
  auto block_iter = leaf.find_block_containing_key(kv_scanner.min_key_);
  auto block_end = leaf.blocks_end();

  if (block_iter == block_end) {
    return;
  }

  // Get the first slice starting from min_key within the first block.
  //
  SlotSlice first_slice = block_iter->items_slice(
      /*key_lower_bound=*/kv_scanner.min_key_,
      /*key_upper_bound=*/None);

  // If the first block's slice is empty, advance to find a non-empty one.
  //
  while (first_slice.empty()) {
    ++block_iter;
    if (block_iter == block_end) {
      return;
    }
    first_slice = block_iter->items_slice();
  }

  this->active_levels_ = 1;
  ScanLevel& level = kv_scanner.scan_levels_.emplace_back(first_slice, this, block_iter, block_end);
  if (kInsertHeap) {
#if TURTLE_KV_PROFILE_QUERIES
    LatencyTimer timer{batt::Every2ToTheConst<8>{},
                       KVStoreScanner::metrics().heap_insert_latency};
#endif
    kv_scanner.heap_.insert(&level);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kInsertHeap>
/*explicit*/ KVStoreScanner::NodeScanState::NodeScanState(
    KVStoreScanner& kv_scanner,
    llfs::PageId page_id,
    std::integral_constant<bool, kInsertHeap>)
    : active_levels_{0}
    , pinned_page_{}
    , node_{nullptr}
    , pivot_i_{0}
    , scan_status_{OkStatus()}
{
  StatusOr<const PackedBlockedLeafPage*> leaf = kv_scanner.block_loader_.set_page(page_id);
  if (!leaf.ok()) {
    this->scan_status_ = leaf.status();
    return;
  }

  this->level_seqs_.emplace_back(
      scan_blocked_leaf(*leaf,
                        &kv_scanner.block_loader_,
                        this->leaf_filter_,
                        kv_scanner.min_key_) |
      batt::seq::status_ok() |
      batt::seq::map([](Slice<const PackedKeyValueSlotPtr> slice) -> EditSlice {
        return EditSlice{slice};
      }) |
      batt::seq::boxed());

  SlotSlice first_slice = this->pull_next(0);
  if (first_slice.empty()) {
    return;
  }

  this->active_levels_ = 1;
  ScanLevel& level =
      kv_scanner.scan_levels_.emplace_back(ScanLevel::BlockedLeafTag{}, first_slice, this);
  if (kInsertHeap) {
#if TURTLE_KV_PROFILE_QUERIES
    LatencyTimer timer{batt::Every2ToTheConst<8>{},
                       KVStoreScanner::metrics().heap_insert_latency};
#endif
    kv_scanner.heap_.insert(&level);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::NodeScanState::NodeScanState() noexcept
    : active_levels_{0}
    , pinned_page_{}
    , node_{nullptr}
    , pivot_i_{0}
    , scan_status_{OkStatus()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 KVStoreScanner::NodeScanState::get_height() const
{
  if (!this->node_) {
    return 1;
  }
  return this->node_->height;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto KVStoreScanner::NodeScanState::pull_next(i32 buffer_level_i) -> SlotSlice
{
  if (this->level_seqs_.empty()) {
    this->deactivate(buffer_level_i);
    return SlotSlice{};
  }

  BoxedSeq<EditSlice>& level_seq = this->level_seqs_[buffer_level_i];

  for (;;) {
    Optional<EditSlice> slice = level_seq.next();
    if (!slice) {
      this->deactivate(buffer_level_i);
      return SlotSlice{};
    }

    SlotSlice result{};
    batt::case_of(
        *slice,
        [](Slice<const EditView>&) {
          BATT_PANIC() << "Invalid EditSlice type: EditView in scan_segmented_level output";
          BATT_UNREACHABLE();
        },
        [&](Slice<const PackedKeyValueSlotPtr>& kv_slice) {
          if (!kv_slice.empty()) {
            result = kv_slice;
          }
        },
        [](Slice<const PackedKeyValue>&) {
          BATT_PANIC() << "Invalid EditSlice type: PackedKeyValue in scan_segmented_level output";
          BATT_UNREACHABLE();
        });

    if (!result.empty()) {
      return result;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStoreScanner::NodeScanState::deactivate(i32 buffer_level_i)
{
  this->active_levels_ &= ~(u64{1} << buffer_level_i);
}

}  // namespace turtle_kv
