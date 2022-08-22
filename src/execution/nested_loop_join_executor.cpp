//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "common/logger.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_executor)),
    right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
    if(left_executor_ != nullptr) left_executor_->Init();
    if(right_executor_ != nullptr) right_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Tuple right_tuple;
    RID right_rid;
    auto predicate = plan_->Predicate();
    // loop
    while(1) {
        if(!is_left_next) {
            if(!left_executor_->Next(&left_tuple,&left_rid)) {
                return false;
            }
            is_left_next = true;
        }
        while(right_executor_->Next(&right_tuple,&right_rid)) {
            auto out_schema1 = left_executor_->GetOutputSchema();
            auto out_schema2 = right_executor_->GetOutputSchema();
            if(predicate != nullptr) {
                auto val = predicate->EvaluateJoin(&left_tuple,out_schema1,&right_tuple,out_schema2);
                if(!val.GetAs<bool>()) continue;
            }

            // return values
            auto outSchema = plan_->OutputSchema();
            auto columns = outSchema->GetColumns();

            std::vector<Value> values;
            values.reserve(columns.size());
            
            for(auto &col : columns) { 
                auto expr_ = col.GetExpr();
                values.emplace_back(expr_->EvaluateJoin(&left_tuple,out_schema1,&right_tuple,out_schema2));
            }
            *tuple = Tuple(values, outSchema);
            // LOG_DEBUG("NestedLoopJoin Scan a Tuple %s", tuple->ToString(outSchema).c_str());
            return true;
        }
        right_executor_->Init();
        is_left_next = false;
    }

    return false; 
}

}  // namespace bustub
