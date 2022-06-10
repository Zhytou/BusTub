//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <unordered_map>
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {
page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; }

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  page_id_ = page_id;
  page->WUnlatch();
}

lsn_t HashTableDirectoryPage::GetLSN() const { return lsn_; }

void HashTableDirectoryPage::SetLSN(lsn_t lsn) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  lsn_ = lsn;
  page->WUnlatch();
}

uint32_t HashTableDirectoryPage::GetGlobalDepth() {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  uint32_t global_depth = global_depth_;
  page->RUnlatch();
  return global_depth;
}

uint32_t HashTableDirectoryPage::GetGlobalDepthMask() {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  uint32_t mask = 1;
  mask = mask << global_depth_;
  page->RUnlatch();
  return mask - 1;
}

void HashTableDirectoryPage::IncrGlobalDepth() {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  global_depth_++;
  page->WUnlatch();
}

void HashTableDirectoryPage::DecrGlobalDepth() {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  global_depth_--;
  page->WUnlatch();
}

page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  page_id_t bucket_page_id = bucket_page_ids_[bucket_idx];
  page->RUnlatch();
  return bucket_page_id;
}

void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  bucket_page_ids_[bucket_idx] = bucket_page_id;
  page->WUnlatch();
}

uint32_t HashTableDirectoryPage::Size() {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  uint32_t size = 1 << global_depth_;
  page->RUnlatch();
  return size;
}

bool HashTableDirectoryPage::CanShrink() {
  // only if all the local depth of the buckets are less than the global depth, the hash table can shrink
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  bool ret = true;
  uint32_t size = Size();
  for (uint32_t i = 0; i < size; i++) {
    if (local_depths_[i] >= global_depth_) {
      ret = false;
      break;
    }
  }
  page->RUnlatch();
  return ret;
}

uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) {
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  uint32_t local_depth = local_depths_[bucket_idx];
  page->RUnlatch();
  return local_depth;
}

void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  local_depths_[bucket_idx] = local_depth;
  page->WUnlatch();
}

void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  local_depths_[bucket_idx]++;
  page->WUnlatch();
}

void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  Page *page = reinterpret_cast<Page *>(this);
  page->WLatch();
  local_depths_[bucket_idx]--;
  page->WUnlatch();
}

uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) {
  // find the split image
  Page *page = reinterpret_cast<Page *>(this);
  page->RLatch();
  uint32_t sibling_bucket_idx = (GetGlobalDepthMask() >> 1) & bucket_idx;
  if (sibling_bucket_idx == bucket_idx && global_depth_ >= 1) {
    sibling_bucket_idx += 1 << (global_depth_ - 1);
  }
  page->RUnlatch();
  return sibling_bucket_idx;
}

/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
 * (3) The LD is the same at each index with the same bucket_page_id
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);

    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_count, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
