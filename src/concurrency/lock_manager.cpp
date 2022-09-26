//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

#include "common/logger.h"

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  if(txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  
  // create a new LockRequest
  LockRequest lockRequest(txn->GetTransactionId(),LockMode::SHARED);
  auto lockRequestQueue = &lock_table_[rid];

  // add into lockRequestQueue
  if(lockRequestQueue->request_queue_.empty()) {
    lockRequest.granted_ = true;
    lockRequestQueue->request_queue_.push_back(lockRequest);
  }else {
    lockRequestQueue->request_queue_.push_back(lockRequest);

    while(!lockRequestQueue->request_queue_.empty()) {
      // find previews LockRequest before txn->txn_id LockRequest
      if(txn->GetState() == TransactionState::ABORTED) {
        return false;
      }
      auto it = lockRequestQueue->request_queue_.begin();
      auto pre = lockRequestQueue->request_queue_.end();
      for(;it != lockRequestQueue->request_queue_.end();++it) {
        if(it->txn_id_ == txn->GetTransactionId()) {
          break;
        }
        pre = it;
      }
      if(it == lockRequestQueue->request_queue_.end()) {
        return false;
      }
      if(pre == lockRequestQueue->request_queue_.end() || (pre->lock_mode_ == LockMode::SHARED && pre->granted_)) {
        it->granted_ = true;
        break;
      }
      // wound wait
      if(pre->lock_mode_ == LockMode::EXCLUSIVE) {

      }
      
      // wait
      lockRequestQueue->cv_.wait(latch);
    }
  }
  lockRequestQueue->cv_.notify_all();
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  if(txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // create a new LockRequest
  LockRequest lockRequest(txn->GetTransactionId(),LockMode::EXCLUSIVE);
  auto lockRequestQueue = &lock_table_[rid];

  if(lockRequestQueue->request_queue_.empty()) {
    lockRequest.granted_ = true;
    lockRequestQueue->request_queue_.push_back(lockRequest);
  }else {
    lockRequestQueue->request_queue_.push_back(lockRequest);
    while(!lockRequestQueue->request_queue_.empty()) {
      // find previews LockRequest before txn->txn_id LockRequest
      if(txn->GetState() == TransactionState::ABORTED) {
        return false;
      }
      auto it = lockRequestQueue->request_queue_.front();
      if(it.txn_id_ == txn->GetTransactionId()) {
        it.granted_ = true;
        break;
      }
      // wait
      lockRequestQueue->cv_.wait(latch);
    }
  }
  lockRequestQueue->cv_.notify_all();
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  
  // upgrading can take place in only the growing phase
  if(txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  auto lockRequestQueue = &lock_table_[rid];

  //  return false if another transaction is already waiting to upgrade their lock.
  if(lockRequestQueue->upgrading_ != INVALID_TXN_ID) 
    return false;
  lockRequestQueue->upgrading_ = txn->GetTransactionId();

  // find a LockRequest that txn_id_ == txn's txn_id
  auto it = lockRequestQueue->request_queue_.begin();
  for(;it != lockRequestQueue->request_queue_.end();++it) {
    if(it->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }
  // not found a LockRequest
  if(it == lockRequestQueue->request_queue_.end()) 
    return false;
  // update LockMode
  it->lock_mode_ = LockMode::EXCLUSIVE;
  it->granted_ = false;
  
  while(!lockRequestQueue->request_queue_.empty()) {
    // txn must be list head
    if(txn->GetState() == TransactionState::ABORTED) {
      return false;
    }

    auto LockRequest = lockRequestQueue->request_queue_.front();
    if(LockRequest.txn_id_ == txn->GetTransactionId()) {
      auto it = lockRequestQueue->request_queue_.begin();
      if(++it == lockRequestQueue->request_queue_.end() || it->granted_ == false) {
        LockRequest.granted_ = true;
        break;
      }
    }
    // wait
    lockRequestQueue->cv_.wait(latch);
  }
  lockRequestQueue->upgrading_ = INVALID_TXN_ID;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);

  if(txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto lockRequestQueue = &lock_table_[rid];
  lockRequestQueue->request_queue_.remove_if([txn](LockRequest l){ return l.txn_id_ == txn->GetTransactionId(); });
  lockRequestQueue->cv_.notify_all();
  
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
