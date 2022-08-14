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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
    AbstractExecutor(exec_ctx),
    plan_(plan),
    curIterator(TableIterator(nullptr, RID(INVALID_PAGE_ID, 0), nullptr)) {}

void SeqScanExecutor::Init() {
    auto catalog = exec_ctx_->GetCatalog();
    meta =  catalog->GetTable(plan_->GetTableOid());
    curIterator = meta->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    auto predicate = plan_->GetPredicate();
    for(;curIterator != meta->table_->End();++curIterator) {
        auto tmp_turple = *curIterator;
        auto val = predicate->Evaluate(&tmp_turple,&meta->schema_);
        if(val.GetAs<bool>()) {
            auto outSchema = plan_->OutputSchema();
            auto columns = outSchema->GetColumns();

            std::vector<Value> values;
            values.reserve(columns.size());
            for(auto &col : columns) {
                values.emplace_back(tmp_turple.GetValue(&meta->schema_, meta->schema_.GetColIdx(col.GetName())));
            }

            *tuple = Tuple(values, outSchema);
            *rid = tmp_turple.GetRid();
 
            curIterator++;
            return true;
        }
    }
    return false; 
}

}  // namespace bustub
