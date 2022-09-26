//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto page = buffer_pool_manager->NewPage(&directory_page_id_);
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());

  // remeber update directory page
  directory_page->IncrGlobalDepth();
  directory_page->SetPageId(directory_page_id_);

  // initially, there should be two buckets
  page_id_t bucket_0_page_id;
  page_id_t bucket_1_page_id;
  buffer_pool_manager_->NewPage(&bucket_0_page_id);
  buffer_pool_manager_->NewPage(&bucket_1_page_id);
  directory_page->SetBucketPageId(0, bucket_0_page_id);
  directory_page->SetLocalDepth(0, 1);
  directory_page->SetBucketPageId(1, bucket_1_page_id);
  directory_page->SetLocalDepth(1, 1);

  // unpin the pages
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_0_page_id, false);
  buffer_pool_manager_->UnpinPage(bucket_1_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/** 
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key,dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  auto directory_page = buffer_pool_manager_->FetchPage(directory_page_id_);
  BUSTUB_ASSERT(directory_page != nullptr,"directory page cannot be nullptr");
  return reinterpret_cast<HashTableDirectoryPage *>(directory_page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> Page * {
  auto bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  BUSTUB_ASSERT(bucket_page != nullptr,"bucket page cannot be nullptr");
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key,directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  auto bucket_page_data = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());

  bucket_page->RLatch();
  // LOG_DEBUG("bucket idx: %d,page id: %d", bucket_idx,bucket_page_id);
  auto success = bucket_page_data->GetValue(key, comparator_, result);
  bucket_page->RUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  

  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  auto directory_page = FetchDirectoryPage();
  // auto bucket_idx = KeyToDirectoryIndex(key, directory_page);
  auto bucket_page_id = KeyToPageId(key,directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  auto bucket_page_data = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  // LOG_DEBUG("Insert():bucket idx: %d,page id: %d", bucket_idx,bucket_page_id);

  bucket_page->WLatch();
  if(bucket_page_data->IsFull()){
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    bucket_page->WUnlatch();
    table_latch_.RUnlock();
    return SplitInsert(transaction, key, value);
  }
  auto success = bucket_page_data->Insert(key, value, comparator_);
  bucket_page->WUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  
  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  auto directory_page = FetchDirectoryPage();
  bool success = false;

  while (1) {
    auto bucket_idx = KeyToDirectoryIndex(key, directory_page);
    auto bucket_page_id = KeyToPageId(key, directory_page);
    auto bucket_page = FetchBucketPage(bucket_page_id);
    auto bucket_page_data = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
    // LOG_DEBUG("bucket idx: %d,page id: %d", bucket_idx,bucket_page_id);

    bucket_page->WLatch();
    if(!bucket_page_data->IsFull()) {
      success = bucket_page_data->Insert(key, value, comparator_);
      buffer_pool_manager_->UnpinPage(bucket_page_id, success);
      // LOG_DEBUG("inset...................");
      bucket_page->WUnlatch();
      break;
    }
    
    // if Directory needs to be expanded
    auto old_local_depth = directory_page->GetLocalDepth(bucket_idx);
    // LOG_DEBUG("LocalDepth: %d,GlobalDepth: %d", old_local_depth,directory_page->GetGlobalDepth());
    if(old_local_depth == directory_page->GetGlobalDepth()) {
      directory_page->IncrGlobalDepth();
    }
    // increase local depth
    directory_page->IncrLocalDepth(bucket_idx);

    // Create an image bucket and initialize the image bucket
    page_id_t image_bucket_page_id;
    auto image_page = buffer_pool_manager_->NewPage(&image_bucket_page_id);
    if(image_page == nullptr) 
      break;
    auto image_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(image_page->GetData());
    auto split_image_bucket_index = directory_page->GetSplitImageIndex(bucket_idx);
    // LOG_DEBUG("split_image_bucket_index: %d", split_image_bucket_index);
    directory_page->SetLocalDepth(split_image_bucket_index, directory_page->GetLocalDepth(bucket_idx));
    directory_page->SetBucketPageId(split_image_bucket_index, image_bucket_page_id);

    // rehash all key-value pairs in the bucket pair
    auto split_image_local_bit = split_image_bucket_index & directory_page->GetLocalDepthMask(bucket_idx);
    // LOG_DEBUG("split_image_local_bit: %x", split_image_local_bit);
    for(uint32_t i = 0;i < BUCKET_ARRAY_SIZE; i++) {
      if (bucket_page_data->IsReadable(i)) {
        auto key = bucket_page_data->KeyAt(i);
        auto which_bucket = Hash(key) & directory_page->GetLocalDepthMask(bucket_idx);
        // LOG_DEBUG("which_bucket: %x", which_bucket);
        if ((which_bucket ^ split_image_local_bit) == 0) {
          // remove from the original bucket and insert the new bucket
          image_bucket_page->Insert(key, bucket_page_data->ValueAt(i), comparator_);
          bucket_page_data->RemoveAt(i);
        }
      }
    }
    bucket_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(image_bucket_page_id, true);

    //  redirect the reset of the buckets.
    uint32_t diff = 1 << directory_page->GetLocalDepth(bucket_idx);
    uint32_t mask = diff - 1;
    for (uint32_t i = bucket_idx & mask; i < directory_page->Size(); i += diff) {
      directory_page->SetBucketPageId(i, bucket_page_id);
      directory_page->SetLocalDepth(i, directory_page->GetLocalDepth(bucket_idx));
    }
    for (uint32_t i = split_image_bucket_index & mask; i < directory_page->Size(); i += diff) {
      directory_page->SetBucketPageId(i, image_bucket_page_id);
      directory_page->SetLocalDepth(i, directory_page->GetLocalDepth(bucket_idx));
    }
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  table_latch_.WUnlock();
  return success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key,directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  auto bucket_page_data = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());

  bucket_page->WLatch();
  auto success = bucket_page_data->Remove(key, value, comparator_);
  bucket_page->WUnlatch();
  
  // if the bucket is empty after removing, call Merge().
  if (success && bucket_page_data->IsEmpty()) {
    Merge(transaction, key, value);
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key,directory_page);
  auto bucket_idx = KeyToDirectoryIndex(key, directory_page);   
  auto image_bucket_index = directory_page->GetSplitImageIndex(bucket_idx);
  // LOG_DEBUG("bucket_idx: %x", bucket_idx);
   // local depth为0说明已经最小了，不收缩
  uint32_t local_depth = directory_page->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    return;
  }

  if (local_depth != directory_page->GetLocalDepth(image_bucket_index)) {
    return;
  }

  auto bucket_page = FetchBucketPage(bucket_page_id);
  auto bucket_page_data = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  bucket_page->RLatch();
  if (!bucket_page_data->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    bucket_page->RUnlatch();
    return;
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->DeletePage(bucket_page_id);

  auto image_bucket_page_id = directory_page->GetBucketPageId(image_bucket_index);
  directory_page->SetBucketPageId(bucket_idx, image_bucket_page_id);
  directory_page->DecrLocalDepth(bucket_idx);
  directory_page->DecrLocalDepth(image_bucket_index);

  uint32_t diff = 1 << directory_page->GetLocalDepth(image_bucket_index);
  uint32_t mask = diff - 1;
  for (uint32_t i = image_bucket_index & mask; i < directory_page->Size(); i += diff) {
    directory_page->SetBucketPageId(i, image_bucket_page_id);
    directory_page->SetLocalDepth(i, directory_page->GetLocalDepth(image_bucket_index));
  }

  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
