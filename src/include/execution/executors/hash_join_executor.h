//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class SimpleJoinHashTable {
  public:
  /**
   * Construct a new SimpleJoinHashTable instance.
   */
  SimpleJoinHashTable() = default;

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param join_key the key to be inserted
   * @param join_val the value to be inserted
   */
  void InsertTuple(const JoinKey &join_key, const JoinValue &join_val) {
      ht_.insert({join_key, join_val});
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param join_key the key to be inserted
   */
  auto FindTuple(const JoinKey &join_key) -> std::vector<JoinValue>{
    std::vector<JoinValue> join_values;
    for(auto it = ht_.find(join_key); it != ht_.end();++it) {
      join_values.push_back(it->second);
    }
    return join_values;
  }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<JoinKey, JoinValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const JoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const JoinValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<JoinKey, JoinValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<JoinKey, JoinValue> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  /** Simple join hash table */
  SimpleJoinHashTable aht_;
};

}  // namespace bustub
