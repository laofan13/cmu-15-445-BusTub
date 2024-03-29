//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "common/logger.h"

#include "concurrency/transaction.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), 
    plan_(plan),
    child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    catalog_ = exec_ctx_->GetCatalog();
    table_info_ = catalog_->GetTable(plan_->TableOid());
    table_heap_ = table_info_->table_.get();
    if(child_executor_ != nullptr)
      child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple del_tuple;
  RID del_rid;
  Transaction *transaction = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  while (true) {
    try {
      if (!child_executor_->Next(&del_tuple, &del_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteExecutor:child execute error.");
      return false;
    }

    // add lock
    if (lock_mgr != nullptr) {
      if (transaction->IsSharedLocked(del_rid)) {
        lock_mgr->LockUpgrade(transaction, del_rid);
      } else if (!transaction->IsExclusiveLocked(del_rid)) {
        lock_mgr->LockExclusive(transaction, del_rid);
      }
    }
    
    if (!table_heap_->MarkDelete(del_rid, exec_ctx_->GetTransaction())) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "MarkDelete: failed.");
      return false;
    }

    // delete index
    for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
      auto del_key = del_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(del_key, del_rid, exec_ctx_->GetTransaction());
      // record
      transaction->GetIndexWriteSet()->emplace_back(IndexWriteRecord(
          del_rid, table_info_->oid_, WType::DELETE, del_tuple,del_tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }

    // 解锁
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
      lock_mgr->Unlock(transaction, del_rid);
    }
  }
  return false; 
 }

}  // namespace bustub
