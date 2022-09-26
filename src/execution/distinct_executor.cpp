//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
    if(child_executor_ != nullptr) {
        child_executor_->Init();
        Tuple tuple;
        RID rid;
        auto outSchema = plan_->OutputSchema();
        auto columns = outSchema->GetColumns();
        
        while (child_executor_->Next(&tuple, &rid)) {
            DistinctKey dis_key;
            dis_key.distinct_vals_.reserve(outSchema->GetColumnCount());
            
            for (uint32_t idx = 0; idx < dis_key.distinct_vals_.capacity(); idx++) {
                dis_key.distinct_vals_.push_back(tuple.GetValue(plan_->OutputSchema(), idx));
            }

            if (map_.count(dis_key) == 0) {
                map_.insert({dis_key, tuple});
            }
        }
    }
    iter_ = map_.begin();
}


auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (iter_ == map_.end()) {
        return false;
    }
    
     // return result
    *tuple = iter_->second;
    *rid = iter_->second.GetRid();
    iter_++;
    return true;
}  // namespace bustub
}
