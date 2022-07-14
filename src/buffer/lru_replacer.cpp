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
    if(frameList.empty()) return false; 
    *frame_id = frameList.front();
    frameList.pop_front();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    if(std::find(frameList.begin(),frameList.end(),frame_id) !=  frameList.end()) {
        frameList.remove(frame_id);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    
    if(int(frameList.size()) >= len) return;
    if(std::find(frameList.begin(),frameList.end(),frame_id) ==  frameList.end()) {
        frameList.push_back(frame_id);
    }
}

auto LRUReplacer::Size() -> size_t { return frameList.size(); }

}  // namespace bustub
