//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

#include "type/value_factory.h" 
#include "common/logger.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
    AbstractExecutor(exec_ctx),
    plan_(plan),
    iter_(TableIterator(nullptr, RID(INVALID_PAGE_ID, 0), nullptr)) {}

void SeqScanExecutor::Init() {
    table_info_ =  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    while(1) {
        if(iter_ == table_info_->table_->End())
            return false;
        auto predicate = plan_->GetPredicate();
        if(predicate == nullptr || predicate->Evaluate(&(*iter_),&table_info_->schema_).GetAs<bool>())
            break;
        iter_++;
    }
    
    // return result
    auto output_schema = plan_->OutputSchema();
    auto columns = output_schema->GetColumns();

    // add lock
    LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
    Transaction *txn = GetExecutorContext()->GetTransaction();
    if (lock_mgr != nullptr) {
        if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
            if (!txn->IsSharedLocked(iter_->GetRid()) && !txn->IsExclusiveLocked(iter_->GetRid())) {
                lock_mgr->LockShared(txn, iter_->GetRid());
            }
        }
    }

    std::vector<Value> values;
    values.reserve(columns.size());
    for(auto &col : columns) {
        auto expr = col.GetExpr();
        values.emplace_back(expr->Evaluate(&(*iter_),&table_info_->schema_));
    }

    // release lock
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
        lock_mgr->Unlock(txn, iter_->GetRid());
    }

    *tuple = Tuple(values, output_schema);
    *rid = iter_->GetRid();
    
    iter_++;
    return true;
}

}  // namespace bustub
