#include <turtle_kv/tree/tree_options.hpp>
//

#include <turtle_kv/import/logging.hpp>

#include <llfs/page_layout.hpp>

#include <batteries/env.hpp>

#include <cmath>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ TreeOptions TreeOptions::with_default_values()
{
  static const Self instance_ = Self{}  //
                                    .set_node_size(4 * kKiB)
                                    .set_leaf_size(2 * kMiB);

  return instance_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ TreeOptions::GlobalOptions& TreeOptions::global_options()
{
  static constexpr const char* const kVarName = "TURTLE_KV_PAGE_CACHE_OBSOLETE_HINTS";

  static GlobalOptions* const p_global_options_ = []() {
    static GlobalOptions global_options_;
    global_options_.page_cache_obsolete_hints = batt::getenv_as<bool>(kVarName).value_or(false);
    LOG(INFO) << kVarName << "=" << global_options_.page_cache_obsolete_hints;
    return &global_options_;
  }();

  return *p_global_options_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const TreeOptions& t)
{
  return out << "TreeOptions{.node_size=" << t.node_size()            //
             << ", .leaf_size=" << t.leaf_size()                      //
             << ", .filter_bits_per_key=" << t.filter_bits_per_key()  //
             << ", .filter_page_size=" << t.filter_page_size()        //
             << ", .max_item_size=" << t.max_item_size()              //
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize TreeOptions::leaf_data_size() const
{
  constexpr usize kFixedHeaders = 64 + 32;  // PackedPageHeader + PackedBlockedLeafPage
  const usize leaf_size = this->leaf_size();
  const usize block_size = this->block_size();
  const usize block_capacity = block_size - 8;  // PackedLeafBlock header is 8 bytes
  const usize waste_per_block = std::min<usize>(this->max_item_size() - 1, block_capacity - 1);

  const usize space = leaf_size - kFixedHeaders - (block_size - 1);
  const usize block_count = space / (block_size + 4);
  const usize capacity = block_count * (block_capacity - waste_per_block);

  return capacity;
}

}  // namespace turtle_kv
