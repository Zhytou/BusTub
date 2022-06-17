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

  table_latch_.WLock();
  // initialize the directory page
  assert(buffer_pool_manager_->NewPage(&directory_page_id_) != nullptr);
  buffer_pool_manager->UnpinPage(directory_page_id_, false);

  // initialize the default bucket page(when global depth is 0, there should be one bucket)
  page_id_t bucket_page_id;
  assert(buffer_pool_manager_->NewPage(&bucket_page_id) != nullptr);
  buffer_pool_manager->UnpinPage(directory_page_id_, false);

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->SetBucketPageId(0, bucket_page_id);
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);

  table_latch_.WUnlock();
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
  return Hash(key) & (dir_page->GetGlobalDepthMask());
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

template <typename KeyType, typename ValueType, typename KeyComparator>
uint64_t HASH_TABLE_TYPE::GetBucketSize() const {
  return static_cast<uint64_t>(BUCKET_ARRAY_SIZE);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  bool ret = false;

  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));

  ret = bucket_page->GetValue(key, comparator_, result);

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), false);

  table_latch_.RUnlock();

  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = false;

  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));

  // if KeyToPageId equals to zero which means that the concerning bucket_page is not created
  if (bucket_page->GetPageId() == dir_page->GetPageId()) {
    table_latch_.RUnlock();
    return false;
  }

  if (!bucket_page->IsFull()) {
    ret = bucket_page->Insert(key, value, comparator_);
    // TODO(zhy): should i unpin the dir_page and bucket_page ?
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), true);

    table_latch_.RUnlock();
    return ret;
  }

  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));

  // if the local depth already equals to the global depth, increase the global depth
  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_idx)) {
    dir_page->IncrGlobalDepth();
    // make sure all the bucket_id in dir_page are correctly pointed to the corresponding page(bucket)
    for (uint32_t i = 0; i < dir_page->Size() / 2; i++) {
      uint32_t sibling_i = i + dir_page->Size() / 2;
      dir_page->SetBucketPageId(sibling_i, dir_page->GetBucketPageId(i));
      dir_page->SetLocalDepth(sibling_i, dir_page->GetLocalDepth(i));
    }
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

  // change all the page_ids
  bucket_idx = dir_page->FindFirstBucket(bucket_page->GetPageId());
  while (bucket_idx < dir_page->Size()) {
    // find the split image(sibling bucket) index using getlocalhighbit method
    uint32_t new_bucket_idx = bucket_idx + dir_page->GetLocalHighBit(bucket_idx);

    // make the dir_page point to the new bucket
    dir_page->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

    // change the local depth for both old and new bucket
    dir_page->IncrLocalDepth(bucket_idx);
    dir_page->SetLocalDepth(new_bucket_idx, dir_page->GetLocalDepth(bucket_idx));

    // move to the next bucket
    bucket_idx += dir_page->GetLocalHighBit(bucket_idx);
  }

  // move the records in old bucket into new bucket if them reach the condition
  for (uint32_t i = 0; bucket_page->IsOccupied(i); i++) {
    if (!bucket_page->IsReadable(i)) {
      continue;
    }
    KeyType key_i = bucket_page->KeyAt(i);
    if (static_cast<page_id_t>(KeyToPageId(key_i, dir_page)) == bucket_page->GetPageId()) {
      continue;
    }
    ValueType value_i = bucket_page->ValueAt(i);
    bucket_page->RemoveAt(i);
    new_bucket_page->Insert(key_i, value_i, comparator_);
  }

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_bucket_page->GetPageId(), true);

  table_latch_.WUnlock();

  // insert the key into corresponding bucket depending on hash(key) & global depth mask
  // using Insert method to prevent recursive split
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = false;

  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));

  if (bucket_page->GetPageId() == dir_page->GetPageId()) {
    table_latch_.RUnlock();
    return false;
  }

  ret = bucket_page->Remove(key, value, comparator_);

  if (bucket_page->IsEmpty()) {
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    table_latch_.RLock();
  }

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), ret);

  table_latch_.RUnlock();

  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);

  // find sibling bucket
  uint32_t sibling_bucket_idx = dir_page->GetLocalHighBit(bucket_idx);
  // set the pointer pointing to the sibling bucket in directory page
  dir_page->SetBucketPageId(bucket_idx, dir_page->GetBucketPageId(sibling_bucket_idx));
  // decrease the sibling bucket local depth
  dir_page->DecrLocalDepth(sibling_bucket_idx);
  // set the local depth as the decreased sibling local depth
  dir_page->SetLocalDepth(bucket_idx, dir_page->GetLocalDepth(sibling_bucket_idx));

  if (dir_page->CanShrink()) {
    // change the pointer in the directory page
    // for the bucket whose index in direcatory page higher than half of the current hash table size(1 << global
    // depth), they should move their contents into their sibling buckets or just change the pointer in the directory
    // page if their sibling page does not exist(page id == 0)

    for (uint32_t i = dir_page->Size() / 2; i < dir_page->Size(); i++) {
      page_id_t bucket_i_page_id = dir_page->GetBucketPageId(i);

      if (bucket_i_page_id == 0) {
        continue;
      }
      uint32_t sibling_i = dir_page->GetLocalHighBit(i);
      page_id_t bucket_sibling_i_page_id = dir_page->GetBucketPageId(sibling_i);
      // if the sibling bucket equals to the bucket, do nothing
      // else if the sibling bucket does not exist, just reuse the bucket
      // otherwise merge them
      if (bucket_i_page_id == bucket_sibling_i_page_id) {
        continue;
      }
      if (bucket_sibling_i_page_id == 0) {
        // reuse the bucket
        dir_page->SetBucketPageId(sibling_i, bucket_i_page_id);
      } else {
        HASH_TABLE_BUCKET_TYPE *bucket_i = FetchBucketPage(bucket_i_page_id);
        HASH_TABLE_BUCKET_TYPE *bucket_sibling_i = FetchBucketPage(bucket_sibling_i_page_id);

        // insert every record into the sibling bucket
        for (uint32_t record_i = 0; bucket_i->IsOccupied(record_i); record_i++) {
          if (!bucket_i->IsReadable(record_i)) {
            continue;
          }
          KeyType k = bucket_i->KeyAt(record_i);
          ValueType v = bucket_i->ValueAt(record_i);
          bucket_sibling_i->Insert(k, v, comparator_);
          bucket_i->RemoveAt(record_i);
        }
        // decrese the local depth
        dir_page->SetLocalDepth(sibling_i, dir_page->GetLocalDepth(i) - 1);

        buffer_pool_manager_->UnpinPage(bucket_i_page_id, true);
        buffer_pool_manager_->UnpinPage(bucket_sibling_i_page_id, true);
      }

      // reset the old bucket metainfo to 0
      dir_page->SetBucketPageId(i, 0);
      dir_page->SetLocalDepth(i, 0);
    }

    // decrease the global depth
    dir_page->DecrGlobalDepth();
  }

  table_latch_.WUnlock();
}

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
