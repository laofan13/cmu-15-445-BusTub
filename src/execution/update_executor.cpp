//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "common/logger.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)){}

void UpdateExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();

  if(child_executor_ != nullptr)
    child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  Tuple old_tuple;
  Tuple new_tuple;
  RID old_rid;

  Transaction *transaction = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  while (1) {
    try {
      if (!child_executor_->Next(&old_tuple, &old_rid)) {
        break;
      }
    } catch (Exception &e) { 
      throw Exception(ExceptionType::UNKNOWN_TYPE, "UpdateExecutor:child execute error.");
      return false;
    }

     // add lock
    if (lock_mgr != nullptr) {
      if (transaction->IsSharedLocked(old_rid)) {
        lock_mgr->LockUpgrade(transaction, old_rid);
      } else if (!transaction->IsExclusiveLocked(old_rid)) {
        lock_mgr->LockExclusive(transaction, old_rid);
      }
    }

    new_tuple = GenerateUpdatedTuple(old_tuple);
    if (!table_heap_->UpdateTuple(new_tuple, old_rid, exec_ctx_->GetTransaction())) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "UpdateTuple: failed.");
      return false;
    }

    // update index
    for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
      // del old index
      auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());

      // add new index_key
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(new_key, old_rid, exec_ctx_->GetTransaction()); 

      // record
      transaction->GetIndexWriteSet()->emplace_back(IndexWriteRecord(
          old_rid, table_info_->oid_, WType::UPDATE, new_tuple,old_tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }

    // 解锁
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
      lock_mgr->Unlock(transaction, old_rid);
    }
  }
  return false; 
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
