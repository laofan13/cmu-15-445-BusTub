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
    if(left_executor_ != nullptr) left_executor_->Init();
    if(right_executor_ != nullptr) {
        right_executor_->Init();
        Tuple tuple;
        RID rid;
        while(right_executor_->Next(&tuple,&rid)) {
            auto right_key_expression = plan_->RightJoinKeyExpression();
            auto right_key_value = right_key_expression->Evaluate(&tuple,right_executor_->GetOutputSchema());
            JoinKey joinkey{{right_key_value}};
            aht_.InsertTuple(joinkey,tuple);
            // LOG_DEBUG("build SimpleJoinHashTable right_key_value %s,Tuple %s",right_key_value.ToString().c_str(), tuple.ToString(right_executor_->GetOutputSchema()).c_str());
        }
    }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    Tuple left_tuple;
    RID left_rid;
    auto out_schema1 = left_executor_->GetOutputSchema();
    auto out_schema2 = right_executor_->GetOutputSchema();
    auto left_key_expression = plan_->LeftJoinKeyExpression();
    auto right_key_expression = plan_->RightJoinKeyExpression();

    while(left_executor_->Next(&left_tuple,&left_rid)) {
        auto left_key_value = left_key_expression->Evaluate(&left_tuple,out_schema1);
        JoinKey joinkey{{left_key_value}};
        auto join_tuples = aht_.FindTuple(joinkey);

        for(auto &right_tuple :join_tuples) {
            auto right_key_value = right_key_expression->Evaluate(&right_tuple,out_schema2);
            if(left_key_value.CompareEquals(right_key_value) == CmpBool::CmpTrue) {
                // LOG_DEBUG("left_tuple  Tuple %s", left_tuple.ToString(out_schema1).c_str());
                // LOG_DEBUG("right_tuple  Tuple %s", right_tuple.ToString(out_schema2).c_str());
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
                // LOG_DEBUG("HashJoinExecutor Scan a Tuple %s", tuple->ToString(outSchema).c_str());
                return true;
            }
        }
     }

    return false; 
}

}  // namespace bustub
