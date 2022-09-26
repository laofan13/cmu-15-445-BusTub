//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "common/logger.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), 
    plan_(plan),
    child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    catalog_ = exec_ctx_->GetCatalog();
    table_info_ = catalog_->GetTable(plan_->TableOid());
    table_heap_ = table_info_->table_.get();
    if(child_executor_ != nullptr)
      child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple del_tuple;
  RID del_rid;
  while (true) {
    try {
      if (!child_executor_->Next(&del_tuple, &del_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteExecutor:child execute error.");
      return false;
    }
    
    if (!table_heap_->MarkDelete(del_rid, exec_ctx_->GetTransaction())) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "MarkDelete: failed.");
      return false;
    }

    // delete index
    for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
      auto del_key = del_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(del_key, del_rid, exec_ctx_->GetTransaction());
    }
  }
  return false; 
 }

}  // namespace bustub
