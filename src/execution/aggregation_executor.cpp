//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "common/logger.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_(std::move(child)),
    aht_(plan->GetAggregates(),plan->GetAggregateTypes()),
    aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
    if(child_ != nullptr)
        child_->Init();
    Tuple tuple;
    RID rid;
    while(child_->Next(&tuple,&rid)) {
        // build Aggregate Key
        auto agg_key = MakeAggregateKey(&tuple);
        // build Aggregate val
        auto agg_val = MakeAggregateValue(&tuple);
        aht_.InsertCombine(agg_key,agg_val);
    }
    //init iterator
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    auto out_schema = GetOutputSchema();
    auto having = plan_->GetHaving();
    for( ;aht_iterator_ != aht_.End(); ++aht_iterator_) {
        auto agg_key = aht_iterator_.Key();
        auto agg_val = aht_iterator_.Val();
        
        // having by
        if(having != nullptr) {
            auto val = having->EvaluateAggregate(agg_key.group_bys_,agg_val.aggregates_);
            if(!val.GetAs<bool>()) {
                continue;
            }
        }

        auto columns = out_schema->GetColumns();
        std::vector<Value> values;
        values.reserve(columns.size());
        for(auto &col : columns) { 
            auto expr_ = col.GetExpr();
            values.emplace_back(expr_->EvaluateAggregate(agg_key.group_bys_,agg_val.aggregates_));
        }

        *tuple = Tuple(values,out_schema);
        // LOG_DEBUG("Aggregation a Tuple %s", tuple->ToString(out_schema).c_str());
        ++aht_iterator_;
        
        return true;
    }
    
    return false; 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
