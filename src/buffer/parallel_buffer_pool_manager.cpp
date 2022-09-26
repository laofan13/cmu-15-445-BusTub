//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
  : pool_size_(pool_size),
    num_instances_(num_instances),
    vec_BPMIs(num_instances) {
    // Allocate and create individual BufferPoolManagerInstances
    for(uint32_t i =0;i < num_instances;i++) {
      vec_BPMIs[i] = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    }
 }

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for(auto &mpi: vec_BPMIs) {
      delete mpi;
  }
};

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return pool_size_*num_instances_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return vec_BPMIs[page_id % num_instances_];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::unique_lock<std::mutex> latch(latch_);
  uint32_t idx = start_index_;
  do {
    auto page = vec_BPMIs[idx % vec_BPMIs.size()]->NewPage(page_id);
    if (page != nullptr) {
      start_index_ = (start_index_ + 1) % vec_BPMIs.size();
      return page;
    }
    idx = (idx + 1) % vec_BPMIs.size();
  } while (idx != start_index_);
  start_index_ = (start_index_ + 1) % vec_BPMIs.size();
 
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for(auto &bpmi : vec_BPMIs) {
    bpmi->FlushAllPages();
  }
}

}  // namespace bustub
