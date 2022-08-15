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
    table_iterator_(TableIterator(nullptr, RID(INVALID_PAGE_ID, 0), nullptr)) {}

void SeqScanExecutor::Init() {
    auto catalog = exec_ctx_->GetCatalog();
    table_info_ =  catalog->GetTable(plan_->GetTableOid());
    table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    auto predicate = plan_->GetPredicate();
    for(;table_iterator_ != table_info_->table_->End();++table_iterator_) {
        auto tmp_tuple = *table_iterator_;
        // LOG_DEBUG("Scan a Tuple %s", tmp_tuple.ToString(&table_info_->schema_).c_str());
        if(predicate != nullptr) {
             auto val = predicate->Evaluate(&tmp_tuple,&table_info_->schema_);
             if(!val.GetAs<bool>()) continue;
        }
        auto outSchema = plan_->OutputSchema();
        auto columns = outSchema->GetColumns();

        std::vector<Value> values;
        values.reserve(columns.size());
        for(auto &col : columns) {
            values.emplace_back(tmp_tuple.GetValue(&table_info_->schema_, table_info_->schema_.GetColIdx(col.GetName())));
        }
        
        *tuple = Tuple(values, outSchema);
        *rid = tmp_tuple.GetRid();
        table_iterator_++;
        return true;
    }
    return false; 
}

}  // namespace bustub
