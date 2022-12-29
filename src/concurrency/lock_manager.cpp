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
#include "concurrency/transaction_manager.h"

#include "common/logger.h"

namespace bustub {

inline void LockManager::InsertLockQueue(LockRequestQueue *lock_queue, txn_id_t txn_id, LockMode lock_mode,
                                         bool granted) {
  for (auto &it : lock_queue->request_queue_) {
    if (it.txn_id_ == txn_id) {
      it.granted_ = granted;
      return;
    }
  }
  LockRequest lockRequest{txn_id, lock_mode};
  lockRequest.granted_ = granted;
  lock_queue->request_queue_.emplace_back(lockRequest);
}

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);

shareCheck:
  // 找到上锁队列
  auto &lock_queue = lock_table_[rid];
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 事务的隔离级别如果是READ_UNCOMITTED，则不需要共享锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  // 事务状态为SHRINKING时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 已经有锁了
  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  auto it = lock_queue.request_queue_.cbegin();
  while (it != lock_queue.request_queue_.cend() && it->txn_id_ != txn->GetTransactionId()) {
    auto trans = TransactionManager::GetTransaction(it->txn_id_);
    if (it->lock_mode_ == LockMode::SHARED && it->granted_) {
      it++;
      continue;
    }

    if (it->txn_id_ > txn->GetTransactionId() && it->lock_mode_ == LockMode::EXCLUSIVE) {
      it = lock_queue.request_queue_.erase(it);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
      continue;
    }

    InsertLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED, false);
    lock_queue.cv_.wait(latch);
    goto shareCheck;
  }
  // 设置状态
  txn->SetState(TransactionState::GROWING);
  InsertLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED, true);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);

exclusiveCheck:
  // 找到上锁队列
  auto &lock_queue = lock_table_[rid];
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 事务状态为SHRINKING且封锁协议为可重复读时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 已经有锁了
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto it = lock_queue.request_queue_.begin();
  while (it != lock_queue.request_queue_.end() && it->txn_id_ != txn->GetTransactionId()) {
    if (lock_queue.request_queue_.front().txn_id_ == txn->GetTransactionId()) {
      break;
    }

    auto trans = TransactionManager::GetTransaction(it->txn_id_);
    if (it->txn_id_ > txn->GetTransactionId()) {
      // 当前事务是老事务，Abort掉新事物
      it = lock_queue.request_queue_.erase(it);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
      continue;
    }

    InsertLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE, false);
    lock_queue.cv_.wait(latch);
    goto exclusiveCheck;
  }
  // 设置状态
  txn->SetState(TransactionState::GROWING);

  InsertLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE, true);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
upgradeCheck:
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 事务状态为SHRINKING且封锁协议为可重复读时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 如果当前正在上锁就抛异常
  if (lock_queue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  // 标记当前正在上锁
  lock_queue.upgrading_ = txn->GetTransactionId();
  // 遍历队列
  auto it = lock_queue.request_queue_.begin();
  while (it != lock_queue.request_queue_.end()) {
    if (lock_queue.request_queue_.size() == 1 && it->txn_id_ == txn->GetTransactionId()) {
      break;
    }

    auto trans = TransactionManager::GetTransaction(it->txn_id_);
    if (it->txn_id_ > txn->GetTransactionId()) {
      // 当前事务是老事务，Abort掉新事物
      it = lock_queue.request_queue_.erase(it);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
      continue;
    }

    lock_queue.cv_.wait(latch);
    goto upgradeCheck;
  }
  // 升级锁
  txn->SetState(TransactionState::GROWING);
  LockRequest &request_item = lock_queue.request_queue_.front();
  assert(request_item.txn_id_ == txn->GetTransactionId());
  request_item.lock_mode_ = LockMode::EXCLUSIVE;

  lock_queue.upgrading_ = INVALID_TXN_ID;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> ul(latch_);
  // 找到上锁队列
  auto &lock_queue = lock_table_[rid];

  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }

  lock_queue.request_queue_.remove_if([txn](LockRequest l) { return l.txn_id_ == txn->GetTransactionId(); });

  lock_queue.cv_.notify_all();

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
