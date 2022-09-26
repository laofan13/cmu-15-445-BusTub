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

    std::vector<Value> values;
    values.reserve(columns.size());
    for(auto &col : columns) {
        auto expr = col.GetExpr();
        values.emplace_back(expr->Evaluate(&(*iter_),&table_info_->schema_));
    }

    *tuple = Tuple(values, output_schema);
    *rid = iter_->GetRid();
    
    iter_++;
    return true;
}

}  // namespace bustub
