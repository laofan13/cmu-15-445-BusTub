//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"
#include "common/logger.h"

#include <thread>

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  // LOG_INFO("Flush Page %d",page_id);
  std::lock_guard<std::mutex> lck(latch_);
  if(page_table_.find(page_id) == page_table_.end())  return false;
  auto frame_id = page_table_[page_id];
  auto page = &pages_[static_cast<int>(frame_id)];
  disk_manager_->WritePage(page_id,page->data_);

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for(auto &entry : page_table_) {
    FlushPgImp(entry.first);
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  Page *page;
  // 0.   Make sure you call AllocatePage!
  auto allo_page_id = AllocatePage();

  std::lock_guard<std::mutex> lck(latch_);
   
  // LOG_INFO("New a Page: %d",allo_page_id);
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  size_t i = 0;
  while(i < pool_size_ && pages_[static_cast<int>(i)].GetPinCount() > 0) i++;
  if(i == pool_size_) return nullptr;

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id;
  if(!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = &pages_[static_cast<int>(frame_id)];
  }else {
    if(!replacer_->Victim(&frame_id)) return nullptr;
    page = &pages_[static_cast<int>(frame_id)];
    page_table_.erase(page->page_id_);
    if(page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_,page->GetData());
      // LOG_INFO("WritePage %d: %s",page->page_id_,page->GetData());
    }
  }
  // LOG_INFO("find a replacement frame %d from either the free list",frame_id);
  
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page->page_id_ = allo_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();

  replacer_->Pin(allo_page_id);
  
  page_table_[allo_page_id] = frame_id;

  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = allo_page_id;
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  Page *page;

  std::lock_guard<std::mutex> lck(latch_);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // LOG_INFO("Fetch page: %d",page_id);
  if(page_table_.find(page_id) != page_table_.end()) {
    // LOG_INFO("found page: %d in page_table",page_id);
    frame_id = page_table_[page_id];
    page = &pages_[static_cast<int>(frame_id)];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }else {
    if(!free_list_.empty()) {
      frame_id = free_list_.back();
      free_list_.pop_back();
    }else {
      if(!replacer_->Victim(&frame_id)) return nullptr;
    }
  }
  // LOG_INFO("Found a R frame %d",frame_id);
  page = &pages_[static_cast<int>(frame_id)];
  // 2.     If R is dirty, write it back to the disk.
  if(page->is_dirty_) {
     disk_manager_->WritePage(page->page_id_,page->GetData());
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->page_id_);
  page_table_[page_id] = frame_id;
  
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  replacer_->Pin(page_id);

  disk_manager_->ReadPage(page_id,page->data_);
  // LOG_INFO("ReadPage %d:%s",page_id,page->data_);

  return page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lck(latch_);
  // 0.   Make sure you call DeallocatePage!
  DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if(page_table_.find(page_id) == page_table_.end())  return true;

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  auto frame_id = page_table_[page_id];
  auto page = &pages_[static_cast<int>(frame_id)]; 

  if(page->GetPinCount() > 0) return false;
  // LOG_INFO("Delete Page %d",page_id);

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();

  free_list_.push_back(frame_id);

  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  // for(auto item: page_table_) {
  //   LOG_INFO("page_table_[%d]=%d", item.first,item.second);
  // }
  std::lock_guard<std::mutex> lck(latch_);
  if(page_table_.find(page_id) == page_table_.end())  return false;
  auto frame_id = page_table_[page_id];
  auto page = &pages_[static_cast<int>(frame_id)];

  // LOG_INFO("UnpinPgImp(%d): frame_id %d", page_id,frame_id);
  if(page->pin_count_ <= 0) return false;

  page->is_dirty_ = is_dirty;
  if(--page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  } 
  return true; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
