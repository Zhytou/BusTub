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
  assert(buffer_pool_manager_->NewPage(&directory_page_id_) != nullptr);
  buffer_pool_manager->UnpinPage(directory_page_id_, false);
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
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), false);
  table_latch_.RUnlock();
  return bucket_page->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = false;

  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));

  if (!bucket_page->IsFull()) {
    table_latch_.RUnlock();
    ret = bucket_page->Insert(key, value, comparator_);
    // TODO(zhy): should i unpin the dir_page and bucket_page ?
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), true);
    return ret;
  }

  table_latch_.RUnlock();
  table_latch_.WLock();

  // if the local depth already equals to the global depth, increase the global depth
  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_idx)) {
    dir_page->IncrGlobalDepth();
  }

  // fetch a new page for the new bucket
  page_id_t new_bucket_page_id;
  // make sure the new page is created
  if (buffer_pool_manager_->NewPage(&new_bucket_page_id) == nullptr) {
    table_latch_.WUnlock();
    return false;
  }
  buffer_pool_manager_->UnpinPage(new_bucket_page_id, false);

  HASH_TABLE_BUCKET_TYPE *new_bucket_page = FetchBucketPage(new_bucket_page_id);
  uint32_t new_bucket_idx = KeyToDirectoryIndex(key, dir_page);

  // make the dir_page point to the new bucket
  dir_page->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

  // change the local depth for both old and new bucket
  dir_page->IncrLocalDepth(bucket_idx);
  dir_page->SetLocalDepth(new_bucket_idx, dir_page->GetLocalDepth(bucket_idx));

  // move the records in old bucket into new bucket if it reaches the condition
  for (uint32_t i = 0; bucket_page->IsOccupied(i); i++) {
    if (!bucket_page->IsReadable(i)) {
      continue;
    }
    KeyType key_i = bucket_page->KeyAt(i);
    if (KeyToDirectoryIndex(key_i, dir_page) == bucket_idx) {
      continue;
    }
    ValueType value_i = bucket_page->ValueAt(i);
    bucket_page->RemoveAt(i);
    new_bucket_page->Insert(key_i, value_i, comparator_);
  }
  // insert the ket into new bucket
  ret = new_bucket_page->Insert(key, value, comparator_);
  table_latch_.WUnlock();

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_bucket_page->GetPageId(), true);

  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = false;

  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  ret = bucket_page->Remove(key, value, comparator_);

  if (bucket_page->IsEmpty()) {
    uint32_t sibling_bucket_idx = dir_page->GetLocalHighBit(bucket_idx);
    dir_page->SetBucketPageId(bucket_idx, dir_page->GetBucketPageId(sibling_bucket_idx));
  }

  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  table_latch_.RUnlock();

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), ret);
  return ret;
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
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
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
