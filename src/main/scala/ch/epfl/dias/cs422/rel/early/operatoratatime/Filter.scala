package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    val nextCol = input.execute()

    if (nextCol.last.isEmpty) {
      nextCol
    } else {
      val activeTup = nextCol.take(nextCol.length - 1).transpose.map(tuple => predicate(tuple))
      val andActiveTup = activeTup.zip(nextCol.last).map(t => t._1 && t._2.asInstanceOf[Boolean])
      nextCol.take(nextCol.length - 1) :+ andActiveTup
    }
  }
}
