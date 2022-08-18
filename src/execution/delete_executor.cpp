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
    auto catalog = exec_ctx_->GetCatalog();
    table_info_ = catalog->GetTable(plan_->TableOid());
    indexes_ = catalog->GetTableIndexes(table_info_->name_);
    
    if(child_executor_ != nullptr) { 
        child_executor_->Init();
    }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    while(child_executor_->Next(tuple,rid)) {
        LOG_DEBUG("Delete a Tuple %s", tuple->ToString(&table_info_->schema_).c_str());

        if(!table_info_->table_->MarkDelete(*rid,exec_ctx_->GetTransaction()))
            throw Exception("Failed to Delete tuple:" + tuple->ToString(&table_info_->schema_));
        // update index 
        for(auto index:indexes_) {
            auto keyAttrs = index->index_->GetKeyAttrs();
            auto index_key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, keyAttrs);
            // del previous index_key
            index->index_->DeleteEntry(index_key,*rid,exec_ctx_->GetTransaction());
        }
    }
    return false; 
 }

}  // namespace bustub
