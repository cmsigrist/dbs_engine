package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Drop]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Drop protected(
                         input: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
                       ) extends skeleton.Drop[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](input)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var currTuple: Option[LateTuple] = NilLateTuple

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    currTuple = input.next()
    currTuple match {
      case NilLateTuple => NilLateTuple
      case Some(lateTuple) => Option(lateTuple.value)
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
