//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "common/logger.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_child)),
    right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
    if(left_executor_!= nullptr) {
        left_executor_->Init();
        Tuple tuple;
        RID rid;
        while(left_executor_->Next(&tuple,&rid)) {
            auto expr = plan_->LeftJoinKeyExpression();
            JoinKey joinkey{{expr->Evaluate(&tuple,left_executor_->GetOutputSchema())}};
            jht_.InsertTuple(joinkey,tuple);
        }
    }
    if(right_executor_ != nullptr) right_executor_->Init();
    it_ = result_.cend();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    auto out_schema1 = left_executor_->GetOutputSchema();
    auto out_schema2 = right_executor_->GetOutputSchema();
    // LOG_DEBUG("..........");
    while(it_ == result_.cend()) {
        try {
            if (!right_executor_->Next(&right_tuple,&right_rid)) {
                return false;
            }
        } catch (Exception &e) { 
            throw Exception(ExceptionType::UNKNOWN_TYPE, "HashJoinExecutor:child execute error.");
            return false;
        }
        
        JoinKey joinkey{plan_->RightJoinKeyExpression()->Evaluate(&right_tuple,out_schema2)};
        result_ = jht_.FindTuple(joinkey);
        it_ = result_.cbegin();
    } 
    // return result
    auto outSchema = plan_->OutputSchema();
    auto columns = outSchema->GetColumns();

    std::vector<Value> values;
    values.reserve(columns.size());
    for(auto &col : columns) { 
        auto expr_ = col.GetExpr();
        values.emplace_back(expr_->EvaluateJoin(&(*it_),out_schema1,&right_tuple,out_schema2));
    }
    *tuple = Tuple(values, outSchema);
    it_++;
    return true; 
}

}  // namespace bustub
