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
  directory_page_id_ = INVALID_PAGE_ID;
  assert(buffer_pool_manager_->NewPage(&directory_page_id_) != nullptr);
  assert(directory_page_id_ == 0);
  assert(buffer_pool_manager->UnpinPage(directory_page_id_, false));

  // initialize the default bucket page(when global depth is 0, there should be one bucket)
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  assert(buffer_pool_manager_->NewPage(&bucket_page_id) != nullptr);
  assert(bucket_page_id == 1);
  assert(buffer_pool_manager->UnpinPage(bucket_page_id, false));

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(dir_page->GetGlobalDepth() == 0);
  dir_page->SetPageId(directory_page_id_);
  dir_page->SetBucketPageId(0, bucket_page_id);
  dir_page->SetLocalDepth(0, 0);
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));

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
  Page *page = reinterpret_cast<Page *>(bucket_page);

  page->RLatch();
  ret = bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), false));

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
  Page *page = reinterpret_cast<Page *>(bucket_page);

  page->WLatch();
  if (!(bucket_page->IsFull())) {
    ret = bucket_page->Insert(key, value, comparator_);
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), true));

    page->WUnlatch();
    table_latch_.RUnlock();
    return ret;
  }
  page->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), false));
  table_latch_.RUnlock();
  // LOG_DEBUG("bucket %d splits", KeyToDirectoryIndex(key, dir_page));
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));

  if (dir_page->GetLocalHighBit(bucket_idx) > DIRECTORY_ARRAY_SIZE) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), false));
    table_latch_.WUnlock();
    LOG_DEBUG("unable to split the bucket, reaching the directory page max capacity");
    return false;
  }

  // if the local depth already equals to the global depth, increase the global depth
  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_idx)) {
    dir_page->IncrGlobalDepth();
    assert(dir_page->GetGlobalDepth() > dir_page->GetLocalDepth(bucket_idx));
    // make sure all the bucket_id in dir_page are correctly pointed to the corresponding page(bucket)
    for (uint32_t i = 0; i < dir_page->Size() / 2; i++) {
      uint32_t sibling_i = i + dir_page->Size() / 2;
      dir_page->SetBucketPageId(sibling_i, dir_page->GetBucketPageId(i));
      dir_page->SetLocalDepth(sibling_i, dir_page->GetLocalDepth(i));
    }
    LOG_DEBUG("hash table grows, increased global depth is %d", dir_page->GetGlobalDepth());
  }

  // fetch a new page for the new bucket
  page_id_t split_bucket_page_id = INVALID_PAGE_ID;
  // make sure the new page is created
  if (buffer_pool_manager_->NewPage(&split_bucket_page_id) == nullptr) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), false));
    table_latch_.WUnlock();
    LOG_DEBUG("fail to create a new page for the split bucket, the buffer pool manager size is %d ",
              static_cast<int>(buffer_pool_manager_->GetPoolSize()));
    dir_page->VerifyIntegrity();
    dir_page->PrintDirectory();
    return false;
  }
  assert(split_bucket_page_id != INVALID_PAGE_ID);
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, false));

  HASH_TABLE_BUCKET_TYPE *split_bucket_page = FetchBucketPage(split_bucket_page_id);

  // change all the page_ids
  bucket_idx = dir_page->FindFirstBucket(bucket_page->GetPageId());
  while (bucket_idx < dir_page->Size()) {
    // find the split image(sibling bucket) index using getlocalhighbit method
    uint32_t split_bucket_idx = bucket_idx ^ dir_page->GetLocalHighBit(bucket_idx);

    // point to the new bucket
    dir_page->SetBucketPageId(split_bucket_idx, split_bucket_page_id);

    // change the local depth for both old and new bucket
    dir_page->IncrLocalDepth(bucket_idx);
    dir_page->IncrLocalDepth(split_bucket_idx);

    // move to the next old bucket
    bucket_idx += dir_page->GetLocalHighBit(bucket_idx);
  }

  Page *page1 = reinterpret_cast<Page *>(bucket_page);
  Page *page2 = reinterpret_cast<Page *>(split_bucket_page);

  page1->WLatch();
  page2->WLatch();
  // move the records in old bucket into new bucket if them reach the condition
  for (uint32_t record_i = 0; bucket_page->IsOccupied(record_i); record_i++) {
    if (!bucket_page->IsReadable(record_i)) {
      continue;
    }
    KeyType key_i = bucket_page->KeyAt(record_i);
    if (static_cast<page_id_t>(KeyToPageId(key_i, dir_page)) == bucket_page->GetPageId()) {
      continue;
    }
    ValueType value_i = bucket_page->ValueAt(record_i);
    bucket_page->RemoveAt(record_i);
    split_bucket_page->Insert(key_i, value_i, comparator_);
  }
  page1->WUnlatch();
  page2->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page->GetPageId(), true));

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
  Page *page = reinterpret_cast<Page *>(bucket_page);

  page->WLatch();
  ret = bucket_page->Remove(key, value, comparator_);
  page->WUnlatch();

  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t merge_bucket_idx = bucket_idx ^ (dir_page->GetLocalHighBit(bucket_idx) >> 1);

  page->RLatch();
  if (bucket_page->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) > 0 &&
      dir_page->GetLocalDepth(bucket_idx) == dir_page->GetLocalDepth(merge_bucket_idx)) {
    page->RUnlatch();
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    //  LOG_DEBUG("bucket %d merges", bucket_idx);
    table_latch_.RLock();
    page->RLatch();
  }
  page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), ret));

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
  uint32_t merge_bucket_idx = bucket_idx ^ (dir_page->GetLocalHighBit(bucket_idx) >> 1);

  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(dir_page->GetBucketPageId(bucket_idx));
  HASH_TABLE_BUCKET_TYPE *merge_bucket_page = FetchBucketPage(dir_page->GetBucketPageId(merge_bucket_idx));

  Page *page1 = reinterpret_cast<Page *>(bucket_page);
  Page *page2 = reinterpret_cast<Page *>(merge_bucket_page);

  page1->RLatch();
  page2->RLatch();

  if ((!bucket_page->IsEmpty() && !merge_bucket_page->IsEmpty()) || dir_page->GetLocalDepth(bucket_idx) <= 0 ||
      dir_page->GetLocalDepth(bucket_idx) != dir_page->GetLocalDepth(merge_bucket_idx)) {
    page1->RUnlatch();
    page2->RUnlatch();

    assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(merge_bucket_page->GetPageId(), false));

    table_latch_.WUnlock();

    return;
  }

  bucket_idx = dir_page->FindFirstBucket(dir_page->GetBucketPageId(bucket_idx));
  while (bucket_idx < dir_page->Size()) {
    //  只有local_depth那一位不一样
    merge_bucket_idx = bucket_idx ^ (dir_page->GetLocalHighBit(bucket_idx) >> 1);

    // point to the bucket that is not empty
    if (bucket_page->IsEmpty()) {
      dir_page->SetBucketPageId(bucket_idx, dir_page->GetBucketPageId(merge_bucket_idx));
    } else {
      assert(merge_bucket_page->IsEmpty());
      dir_page->SetBucketPageId(merge_bucket_idx, dir_page->GetBucketPageId(bucket_idx));
    }

    // change the local depth for both old and new bucket
    dir_page->DecrLocalDepth(bucket_idx);
    dir_page->DecrLocalDepth(merge_bucket_idx);

    // move to the next bucket
    bucket_idx += 2 * dir_page->GetLocalHighBit(bucket_idx);
  }
  page1->RUnlatch();
  page2->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(bucket_page->GetPageId(), false));
  assert(buffer_pool_manager_->UnpinPage(merge_bucket_page->GetPageId(), false));

  if (dir_page->CanShrink() && dir_page->GetGlobalDepth() > 1) {
    // change the pointer in the directory page
    for (uint32_t i = dir_page->Size() / 2; i < dir_page->Size(); i++) {
      page_id_t bucket_i_page_id = dir_page->GetBucketPageId(i);

      uint32_t sibling_i = i - dir_page->Size() / 2;
      page_id_t bucket_sibling_i_page_id = dir_page->GetBucketPageId(sibling_i);
      // the sibling bucket page id must equal to the bucket page id
      assert(bucket_i_page_id == bucket_sibling_i_page_id);
      // reset the old bucket metainfo to 0
      dir_page->SetBucketPageId(i, 0);
      dir_page->SetLocalDepth(i, 0);
    }

    // decrease the global depth
    dir_page->DecrGlobalDepth();

    LOG_DEBUG("hash table shrinks, dcreased global depth is %d", dir_page->GetGlobalDepth());
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();

  Merge(transaction, key, value);
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
