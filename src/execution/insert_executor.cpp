//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

#include "common/logger.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    catalog_ = exec_ctx_->GetCatalog();
    table_info_ = catalog_->GetTable(plan_->TableOid());
    table_heap_ = table_info_->table_.get();
    if(child_executor_ != nullptr)
        child_executor_->Init();
}

void InsertExecutor::InsertTupleWithIndex(Tuple &tuple) {
  RID rid;
  // update tuple
  if (!table_heap_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:no enough space for this tuple.");
  }
  // update index
  for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    auto index_key = tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
    index->index_->InsertEntry(index_key, rid, exec_ctx_->GetTransaction()); 
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    if(plan_->IsRawInsert()) {
        for(auto &values: plan_->RawValues()) {
            Tuple tmp_tuple(values,&table_info_->schema_);
            InsertTupleWithIndex(tmp_tuple);
        }
        return false;
    }

    while (1) {
        Tuple tuple;
        RID rid;
        try {
            if (!child_executor_->Next(&tuple, &rid)) {
                break;
            }
        } catch (Exception &e) { 
            throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor:child execute error.");
            return false;
        }
        InsertTupleWithIndex(tuple);
    }
    return false;
}

}  // namespace bustub
