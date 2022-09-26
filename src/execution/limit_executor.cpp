//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
    count_ = 0;
    if(child_executor_ != nullptr) 
        child_executor_->Init();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Tuple new_tuple;
    RID new_rid;
    try {
        if (!child_executor_->Next(&new_tuple, &new_rid)) {
            return false;
        }
    } catch (Exception &e) { 
        throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor:child execute error.");
        return false;
    }

    if(++count_ > plan_->GetLimit()) return false;

    // return result
    auto scan_schema = child_executor_->GetOutputSchema();
    auto outSchema = plan_->OutputSchema();
    auto columns = outSchema->GetColumns();

    std::vector<Value> values;
    values.reserve(columns.size());
    for(auto &col : columns) { 
        auto expr = col.GetExpr();
        values.emplace_back(expr->Evaluate(&new_tuple,scan_schema));
    }
    *tuple = Tuple(values, outSchema);
    *rid = new_rid;
    return true;
}

}  // namespace bustub
