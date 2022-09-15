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
  buffer_pool_manager->NewPage(&directory_page_id_);
  auto directory_page = FetchDirectoryPage();
  directory_page->SetPageId(directory_page_id_);
  directory_page->IncrGlobalDepth();

  page_id_t bucket_page_id;
  buffer_pool_manager->NewPage(&bucket_page_id);
  directory_page->SetBucketPageId(0,bucket_page_id);
  directory_page->SetLocalDepth(0,1);

  buffer_pool_manager->NewPage(&bucket_page_id);
  directory_page->SetBucketPageId(1,bucket_page_id);
  directory_page->SetLocalDepth(1,1);
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
  if(directory_page == nullptr) return nullptr;
  return reinterpret_cast<HashTableDirectoryPage *>(directory_page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  auto bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  if(bucket_page == nullptr) return nullptr;
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
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
  // LOG_DEBUG("bucket idx: %d,page id: %d", bucket_idx,bucket_page_id);

  auto retValue = bucket_page->GetValue(key,comparator_,result);
  table_latch_.RUnlock();
  return retValue;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key,directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  // LOG_DEBUG("bucket idx: %d,page id: %d", bucket_idx,bucket_page_id);

  bool retValue;
  if(bucket_page->IsFull()){
    retValue = SplitInsert(transaction,key,value);
  }else{
    retValue = bucket_page->Insert(key,value,comparator_);
  }
  table_latch_.WUnlock();
  return retValue;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto directory_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key,directory_page);
  auto bucket_page_id = KeyToPageId(key,directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  while(bucket_page->IsFull()) {
    auto globalDepth = directory_page->GetGlobalDepth();
    auto localDepth = directory_page->GetLocalDepth(bucket_idx);
    // LOG_DEBUG("Bucket[%d] is full : GlobalDepth=%d, localDepth=%d",bucket_idx,globalDepth,localDepth);
    if (globalDepth == localDepth) {
      uint32_t len = 1 << globalDepth;
      for (uint32_t i = 0; i < len; i++) {
        directory_page->SetLocalDepth(len + i,directory_page->GetLocalDepth(i));
        directory_page->SetBucketPageId(len + i,directory_page->GetBucketPageId(i));
      }
      directory_page->IncrGlobalDepth();
    }
    globalDepth = directory_page->GetGlobalDepth();

    //create a new bucket_page
    page_id_t new_bucket_page_id;
    buffer_pool_manager_->NewPage(&new_bucket_page_id);
    auto new_bucket_page = FetchBucketPage(new_bucket_page_id);
    
    // If the highest bit of the local depth is 1, allocate a new bucket.otherwise old bucket
    auto localDepthBit = 1 << localDepth;
    auto bucketIndexLowBit = bucket_idx & (localDepthBit - 1);
    uint32_t size = 1 << (globalDepth  - localDepth);
    for(uint32_t i = 0; i < size; i++) {
      auto index = i << localDepth | bucketIndexLowBit;
      if(index & localDepthBit) {
        directory_page->SetBucketPageId(index,new_bucket_page_id);
      }
      directory_page->IncrLocalDepth(index);
    }
  
    // move element from old bucket to new bucket
    for(size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      if(!bucket_page->IsReadable(i)) continue;
      auto old_key = bucket_page->KeyAt(i);
      auto index = KeyToDirectoryIndex(old_key,directory_page);
      if(index & localDepthBit) {
        new_bucket_page->Insert(old_key,bucket_page->ValueAt(i),comparator_);
        bucket_page->RemoveAt(i);
      }
    }

    bucket_idx = KeyToDirectoryIndex(key,directory_page);
    if(bucket_idx & localDepthBit) {
      bucket_page_id = new_bucket_page_id;
      bucket_page = new_bucket_page;
    }
  }

  return bucket_page->Insert(key,value,comparator_);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key,directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  auto retValue = bucket_page->Remove(key,value,comparator_);
  table_latch_.WUnlock();
  return retValue;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

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
