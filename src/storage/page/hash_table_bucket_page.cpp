//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {
template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_BUCKET_TYPE::GetPageId() {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  page_id_t page_id = page->GetPageId();
  page->RUnlatch();
  return page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    char mask = static_cast<char>(1 << (i % 8));

    if ((occupied_[i / 8] & mask) == 0) {
      break;
    }

    if ((readable_[i / 8] & mask) != 0 && !cmp(key, array_[i].first)) {
      result->push_back(array_[i].second);
    }
  }
  page->RUnlatch();
  return !(result->empty());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();

  size_t available_record_idx = BUCKET_ARRAY_SIZE;
  char mask;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    mask = static_cast<char>(1 << (i % 8));

    if ((occupied_[i / 8] & mask) == static_cast<char>(0)) {
      if (available_record_idx == BUCKET_ARRAY_SIZE) {
        available_record_idx = i;
      }
      break;
    }

    if ((readable_[i / 8] & mask) && !cmp(key, array_[i].first) && value == array_[i].second) {
      page->WUnlatch();
      LOG_DEBUG("duplicate values for the same key");
      return false;
    }

    if (available_record_idx == BUCKET_ARRAY_SIZE && (readable_[i / 8] & mask) == static_cast<char>(0)) {
      available_record_idx = i;
    }
  }

  if (available_record_idx == BUCKET_ARRAY_SIZE) {
    page->WUnlatch();
    LOG_DEBUG("the bucket page is full");
    return false;
  }

  mask = static_cast<char>(1 << (available_record_idx % 8));
  array_[available_record_idx].first = key;
  array_[available_record_idx].second = value;
  occupied_[available_record_idx / 8] = occupied_[available_record_idx / 8] | mask;
  readable_[available_record_idx / 8] = readable_[available_record_idx / 8] | mask;

  page->WUnlatch();
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    char mask = static_cast<char>(1 << (i % 8));

    if ((occupied_[i / 8] & mask) == static_cast<char>(0)) {
      break;
    }
    if ((readable_[i / 8] & mask) && !cmp(key, array_[i].first) && value == array_[i].second) {
      readable_[i / 8] = readable_[i / 8] & (static_cast<char>(255) - mask);
      page->WUnlatch();
      return true;
    }
  }
  page->WUnlatch();
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  char mask = static_cast<char>(1 << (bucket_idx % 8));
  readable_[bucket_idx / 8] = readable_[bucket_idx / 8] & (static_cast<char>(255) - mask);
  page->WUnlatch();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  char mask = static_cast<char>(1 << (bucket_idx % 8));
  return (occupied_[bucket_idx / 8] & mask) != static_cast<char>(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  char mask = static_cast<char>(1 << (bucket_idx % 8));
  occupied_[bucket_idx / 8] = occupied_[bucket_idx / 8] | mask;
  page->WUnlatch();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  char mask = static_cast<char>(1 << (bucket_idx % 8));
  return (readable_[bucket_idx / 8] & mask) != static_cast<char>(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  char mask = static_cast<char>(1 << (bucket_idx % 8));
  readable_[bucket_idx / 8] = readable_[bucket_idx / 8] | mask;
  page->WUnlatch();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  bool ret = true;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i) || (IsOccupied(i) && !IsReadable(i))) {
      ret = false;
      break;
    }
  }
  page->RUnlatch();
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  uint32_t ret = 0;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    char mask = static_cast<char>(1 << (i % 8));

    ret += (occupied_[i / 8] & mask) ? 1 : 0;
  }
  page->RUnlatch();
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  bool ret = readable_[0] == static_cast<char>(0);
  page->RUnlatch();
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
