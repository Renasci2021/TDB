#include "include/query_engine/planner/operator/join_physical_operator.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

JoinPhysicalOperator::JoinPhysicalOperator() = default;

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx) {
  trx_ = trx;
  if (left_child_ == nullptr || right_child_ == nullptr) {
    return RC::INVALID_ARGUMENT;
  }
  RC rc = left_child_->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  rc = right_child_->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  while (left_child_->next() == RC::SUCCESS) {
    Tuple *left_tuple = left_child_->current_tuple();
    while (right_child_->next() == RC::SUCCESS) {
      Tuple *right_tuple = right_child_->current_tuple();
      joined_tuple_.set_left(left_tuple);
      joined_tuple_.set_right(right_tuple);
      Value value;
      RC rc = join_condition_->get_value(joined_tuple_, value);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      if (value.get_boolean()) {
        return RC::SUCCESS;
      }
    }
    right_child_->close();
    right_child_->open(trx_);
  }

  return RC::RECORD_EOF;
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  RC rc = left_child_->close();
  if (rc != RC::SUCCESS) {
    return rc;
  }
  rc = right_child_->close();
  if (rc != RC::SUCCESS) {
    return rc;
  }
  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
