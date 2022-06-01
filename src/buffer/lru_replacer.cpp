//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  size_ = 0;
  head_ = num_pages;
  frames_.resize(num_pages);
  for (size_t i = 0; i < num_pages; i++) {
    frames_[i].first = i;
    frames_[i].second = i;
    pinned_frames_.insert(i);
  }
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (size_ == 0) {
    return false;
  }
  size_ -= 1;

  frame_id_t tail = frames_[head_].first;
  frame_id_t ntail = frames_[tail].first;

  pinned_frames_.insert(tail);
  *frame_id = tail;

  frames_[head_].first = ntail;
  frames_[ntail].second = head_;

  frames_[tail].first = tail;
  frames_[tail].second = tail;
  if (size_ == 0) {
    head_ = frames_.size();
  }
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (pinned_frames_.find(frame_id) != pinned_frames_.end()) {
    return;
  }

  pinned_frames_.insert(frame_id);
  size_ -= 1;

  frame_id_t prev = frames_[frame_id].first;
  frame_id_t next = frames_[frame_id].second;

  frames_[prev].second = next;
  frames_[next].first = prev;

  frames_[frame_id].first = frame_id;
  frames_[frame_id].second = frame_id;

  if (head_ == frame_id) {
    if (size_ > 0) {
      head_ = next;
    } else {
      head_ = frames_.size() + 1;
    }
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (pinned_frames_.find(frame_id) == pinned_frames_.end()) {
    return;
  }

  pinned_frames_.erase(frame_id);
  size_ += 1;

  if (head_ < static_cast<frame_id_t>(frames_.size())) {
    frame_id_t tail = frames_[head_].first;
    frames_[tail].second = frame_id;
    frames_[head_].first = frame_id;

    frames_[frame_id].first = tail;
    frames_[frame_id].second = head_;
  }
  head_ = frame_id;
}

size_t LRUReplacer::Size() { return size_; }

}  // namespace bustub
