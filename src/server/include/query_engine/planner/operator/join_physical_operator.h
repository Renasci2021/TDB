#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  JoinPhysicalOperator(
      std::unique_ptr<PhysicalOperator> left_child,   // 左子算子
      std::unique_ptr<PhysicalOperator> right_child,  // 右子算子
      std::unique_ptr<Expression> join_condition)     // join条件
      : left_child_(std::move(left_child)),
        right_child_(std::move(right_child)),
        join_condition_(std::move(join_condition)) {}
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

private:
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  // 当前关联的左右两个tuple
  std::unique_ptr<PhysicalOperator> left_child_, right_child_;
  std::unique_ptr<Expression> join_condition_;
};
