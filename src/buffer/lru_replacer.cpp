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

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    std::unique_lock<std::mutex> latch(latch_);

    if(frame_list_.empty()) {
        *frame_id = INVALID_PAGE_ID;
        return false;
    }
    *frame_id = frame_list_.front();
    frame_list_.pop_front();

    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::unique_lock<std::mutex> latch(latch_);
    auto it = std::find(frame_list_.begin(), frame_list_.end(), frame_id);
    if(it !=  frame_list_.end())
        frame_list_.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::unique_lock<std::mutex> latch(latch_);
    auto it = std::find(frame_list_.begin(), frame_list_.end(), frame_id);
    if(it ==  frame_list_.end())
        frame_list_.push_back(frame_id);
}

auto LRUReplacer::Size() -> size_t { return frame_list_.size(); }

}  // namespace bustub
