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
    Tuple tuple;
    RID rid;
    if(child_ != nullptr){
        child_->Init();
        try {
            while (child_->Next(&tuple, &rid)) {
                aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
            }
        } catch (Exception &e) {
            throw Exception(ExceptionType::UNKNOWN_TYPE, "AggregationExecutor:child execute error.");
        }
        aht_iterator_ = aht_.Begin();
    }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    AggregateKey agg_key;
    AggregateValue agg_val;

    while(1) {
        if (aht_iterator_ == aht_.End()) {
            return false;
        }
        agg_key = aht_iterator_.Key();
        agg_val = aht_iterator_.Val();
        auto having = plan_->GetHaving();
        if (having == nullptr || having->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>())
            break;
        ++aht_iterator_;
    }
    // return result
    auto out_schema = GetOutputSchema();
    auto columns = out_schema->GetColumns();

    std::vector<Value> values;
    values.reserve(columns.size());
    for(auto &col : columns) { 
        auto expr_ = col.GetExpr();
        values.emplace_back(expr_->EvaluateAggregate(agg_key.group_bys_,agg_val.aggregates_));
    }
    *tuple = Tuple(values,out_schema);
    ++aht_iterator_;
    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
