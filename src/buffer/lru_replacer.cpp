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
  size = 0;
  head = num_pages;
  frames.resize(num_pages);
  for (size_t i = 0; i < num_pages; i++) {
    frames[i].first = i;
    frames[i].second = i;
    pinned_frames.insert(i);
  }
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (size == 0)
    return false;
  else
    size -= 1;

  frame_id_t tail = frames[head].first, ntail = frames[tail].first;

  pinned_frames.insert(tail);
  *frame_id = tail;

  frames[head].first = ntail;
  frames[ntail].second = head;

  frames[tail].first = tail;
  frames[tail].second = tail;
  if (size == 0) head = frames.size();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (pinned_frames.find(frame_id) != pinned_frames.end())
    return;
  else
    pinned_frames.insert(frame_id);
  size -= 1;

  frame_id_t prev = frames[frame_id].first, next = frames[frame_id].second;
  frames[prev].second = next;
  frames[next].first = prev;

  frames[frame_id].first = frame_id;
  frames[frame_id].second = frame_id;

  if (head == frame_id) {
    if (size > 0)
      head = next;
    else
      head = frames.size() + 1;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (pinned_frames.find(frame_id) == pinned_frames.end())
    return;
  else
    pinned_frames.erase(frame_id);
  size += 1;

  if (head < static_cast<frame_id_t>(frames.size())) {
    frame_id_t tail = frames[head].first;
    frames[tail].second = frame_id;
    frames[head].first = frame_id;

    frames[frame_id].first = tail;
    frames[frame_id].second = head;
  }
  head = frame_id;
}

size_t LRUReplacer::Size() { return size; }

}  // namespace bustub
