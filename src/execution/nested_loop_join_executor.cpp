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

    while(!left_executor_->Next(&left_tuple,&left_rid));
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Tuple right_tuple;
    RID right_rid;
    auto out_schema1 = left_executor_->GetOutputSchema();
    auto out_schema2 = right_executor_->GetOutputSchema();

    while(1) {
        try {
            if (!right_executor_->Next(&right_tuple,&right_rid)) {
                if (!left_executor_->Next(&left_tuple,&left_rid)) {
                    return false;
                }
                right_executor_->Init();
                continue;
            }
        } catch (Exception &e) { 
            throw Exception(ExceptionType::UNKNOWN_TYPE, "NestedLoopJoinExecutor:child execute error.");
            return false;
        }
        
        auto predicate = plan_->Predicate();
        if(predicate == nullptr || predicate->EvaluateJoin(&left_tuple,out_schema1,&right_tuple,out_schema2).GetAs<bool>()) {
            break;
        }
    }
    
    // return result
    auto out_schema = plan_->OutputSchema();
    auto columns = out_schema->GetColumns();

    std::vector<Value> values;
    values.reserve(columns.size());
    for(auto &col : columns) { 
        auto expr_ = col.GetExpr();
        values.emplace_back(expr_->EvaluateJoin(&left_tuple,out_schema1,&right_tuple,out_schema2));
    }
    *tuple = Tuple(values, out_schema);
    return true;
}

}  // namespace bustub
