//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_plan.h
//
// Identification: src/include/execution/plans/hash_join_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Hash join performs a JOIN operation with a hash table.
 */
class HashJoinPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new HashJoinPlanNode instance.
   * @param output_schema The output schema for the JOIN
   * @param children The child plans from which tuples are obtained
   * @param left_key_expression The expression for the left JOIN key
   * @param right_key_expression The expression for the right JOIN key
   */
  HashJoinPlanNode(const Schema *output_schema, std::vector<const AbstractPlanNode *> &&children,
                   const AbstractExpression *left_key_expression, const AbstractExpression *right_key_expression)
      : AbstractPlanNode(output_schema, std::move(children)),
        left_key_expression_{left_key_expression},
        right_key_expression_{right_key_expression} {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::HashJoin; }

  /** @return The expression to compute the left join key */
  auto LeftJoinKeyExpression() const -> const AbstractExpression * { return left_key_expression_; }

  /** @return The expression to compute the right join key */
  auto RightJoinKeyExpression() const -> const AbstractExpression * { return right_key_expression_; }

  /** @return The left plan node of the hash join */
  auto GetLeftPlan() const -> const AbstractPlanNode * {
    BUSTUB_ASSERT(GetChildren().size() == 2, "Hash joins should have exactly two children plans.");
    return GetChildAt(0);
  }

  /** @return The right plan node of the hash join */
  auto GetRightPlan() const -> const AbstractPlanNode * {
    BUSTUB_ASSERT(GetChildren().size() == 2, "Hash joins should have exactly two children plans.");
    return GetChildAt(1);
  }

 private:
  /** The expression to compute the left JOIN key */
  const AbstractExpression *left_key_expression_;
  /** The expression to compute the right JOIN key */
  const AbstractExpression *right_key_expression_;
};

/** JoinKey represents a key in an join operation */
struct JoinKey {
  /** The group-by values */
   Value join_value_;

  /**
   * Compares two join keys for equality.
   * @param other the other join key to be compared with
   * @return `true` if both join keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool {
    return join_value_.CompareEquals(other.join_value_) == CmpBool::CmpTrue;
  }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    if (!agg_key.join_value_.IsNull()) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&agg_key.join_value_));
    }
    return curr_hash;
  }
};
}  // namespace bustub
