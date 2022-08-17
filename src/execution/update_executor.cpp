//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "common/logger.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)){}

void UpdateExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  indexes_ = catalog->GetTableIndexes(table_info_->name_);

  if(child_executor_ != nullptr) 
    child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  while(child_executor_->Next(tuple,rid)) {
    // LOG_DEBUG("updale a Tuple %s", tuple->ToString(&table_info_->schema_).c_str());
    auto tmp_tuple = GenerateUpdatedTuple(*tuple);

    if(!table_info_->table_->UpdateTuple(tmp_tuple,*rid,exec_ctx_->GetTransaction()))
        throw Exception("Failed to Update tuple:" + tmp_tuple.ToString(&table_info_->schema_));
    // update index 
    for(auto index:indexes_) {
        auto keyAttrs = index->index_->GetKeyAttrs();
        auto index_key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, keyAttrs);
        // del previous index_key
        index->index_->DeleteEntry(index_key,*rid,exec_ctx_->GetTransaction());
        // add new index_key
        index_key = tmp_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, keyAttrs);
        index->index_->InsertEntry(index_key, *rid, exec_ctx_->GetTransaction()); 
    }
  }
  return false; 
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
