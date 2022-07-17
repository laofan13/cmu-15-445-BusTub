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

LRUReplacer::LRUReplacer(size_t num_pages):len(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    latch_.lock();

    if(frameList.empty()) return false; 
    *frame_id = frameList.front();
    frameList.pop_front();

    latch_.unlock();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    latch_.lock();
    if(std::find(frameList.begin(),frameList.end(),frame_id) !=  frameList.end()) {
        frameList.remove(frame_id);
    }
    latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch_.lock();
    if(int(frameList.size()) >= len) return;
    if(std::find(frameList.begin(),frameList.end(),frame_id) ==  frameList.end()) {
        frameList.push_back(frame_id);
    }
    latch_.unlock();
}

auto LRUReplacer::Size() -> size_t { return frameList.size(); }

}  // namespace bustub
