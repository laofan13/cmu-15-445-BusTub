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

LRUReplacer::LRUReplacer(size_t num_pages):num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    std::unique_lock<std::mutex> latch(latch_);

    if(frame_list_.empty()) return false; 
    *frame_id = frame_list_.front();
    frame_list_.pop_front();

    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::unique_lock<std::mutex> latch(latch_);
    if(std::find(frame_list_.begin(), frame_list_.end(), frame_id) !=  frame_list_.end())
        frame_list_.remove(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::unique_lock<std::mutex> latch(latch_);
    if(frame_list_.size() >= num_pages_) return;
    if(std::find(frame_list_.begin(), frame_list_.end(), frame_id) ==  frame_list_.end())
        frame_list_.push_back(frame_id);
}

auto LRUReplacer::Size() -> size_t { return frame_list_.size(); }

}  // namespace bustub
