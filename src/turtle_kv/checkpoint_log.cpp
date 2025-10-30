#include <turtle_kv/checkpoint_log.hpp>
//

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/optional.hpp>

#include <llfs/volume_config.hpp>

#include <batteries/seq/first.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status create_checkpoint_log(llfs::StorageContext& storage_context,
                             const TreeOptions& tree_options,
                             const std::filesystem::path& file_name) noexcept
{
  const llfs::VolumeConfigOptions volume_config_options{
      .base =
          llfs::VolumeOptions{
              .name = "checkpoint_log",
              .uuid = None,
              .max_refs_per_page = tree_options.max_page_refs_per_node(),
              .trim_lock_update_interval = llfs::TrimLockUpdateInterval{64 * kKiB},
              .trim_delay_byte_count = llfs::TrimDelayByteCount{0},
          },
      .root_log =
          llfs::LogDeviceConfigOptions2{
              .uuid = None,
              .log_size = 2 * kMiB,
              .device_page_size_log2 = None,
              .data_alignment_log2 = None,
          },
      .recycler_max_buffered_page_count = llfs::PageCount{65536},
  };

  boost::uuids::uuid checkpoint_log_volume_uuid;
  {
    Status file_create_status = storage_context.add_new_file(
        file_name.string(),
        [&](llfs::StorageFileBuilder& builder) -> Status {
          //----- --- -- -  -  -   -
          llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedVolumeConfig&>>
              p_checkpoint_volume_config = builder.add_object(volume_config_options);

          checkpoint_log_volume_uuid = (*p_checkpoint_volume_config)->uuid;

          return OkStatus();
        });

    BATT_REQUIRE_OK(file_create_status);
  }

  return OkStatus();
}

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
// TODO: Consider having this function return Volume and Checkpoints
//
StatusOr<std::unique_ptr<llfs::Volume>> open_checkpoint_log(
    llfs::StorageContext& storage_context,
    const std::filesystem::path& file_name) noexcept
{
  LOG(INFO) << "Entering open_checkpoint_log";
  BATT_REQUIRE_OK(storage_context.add_existing_named_file(file_name.string()));

  LOG(INFO) << "open_checkpoint_log 1";

  Optional<batt::SharedPtr<llfs::StorageObjectInfo>> info =
      storage_context                                                                      //
          .find_objects_by_tag(llfs::PackedConfigTagFor<llfs::PackedVolumeConfig>::value)  //
      | batt::seq::first();

  if (!info) {
    return Status{batt::StatusCode::kNotFound};
  }

  LOG(INFO) << "open_checkpoint_log 2";

  llfs::PackedUUID uuid = (**info).p_config_slot->uuid;

  auto root_log_options =                                   //
      llfs::LogDeviceRuntimeOptions::with_default_values()  //
          .set_name("checkpoint_log");

  LOG(INFO) << "open_checkpoint_log 3";

  auto recycler_log_options =                               //
      llfs::LogDeviceRuntimeOptions::with_default_values()  //
          .set_name("checkpoint_recycler");

  LOG(INFO) << "open_checkpoint_log 4";

  return storage_context.recover_object(
      batt::StaticType<llfs::PackedVolumeConfig>{},
      uuid,
      llfs::VolumeRuntimeOptions{
          // TODO: [Gabe Bornstein 10/21/25] SlotVisitorFn needs to be defined to recover a specific
          // PackedVariant type
          // Look at slot_reader.hpp::make_slot_visitor (in template TypedSlotReader).
          // This will adapt the pattern for visit_typed_next we use in recover_packed_checkpoints
          // for the pattern here of SlotVisitorFn.
          // Recovering PackedCheckpoints will be one of the final steps of recovering the volume.
          //
          // Could call this .slot_visitor_fn = [](auto&&...) { return OkStatus(); } if we want to
          // grab all the user slots in the volume. Can sort them out later.
          //
          .slot_visitor_fn =
              [](auto&&...) {
                return OkStatus();
              },
          .root_log_options = root_log_options,
          .recycler_log_options = recycler_log_options,
          .trim_control = nullptr,
      });
}

}  // namespace turtle_kv
