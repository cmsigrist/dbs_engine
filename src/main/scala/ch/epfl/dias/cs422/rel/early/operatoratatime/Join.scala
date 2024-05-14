package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    var joinTuples: IndexedSeq[Column] = Vector()

    val leftCols = left.execute()
    val rightCols = right.execute()

    if (leftCols.last.isEmpty) {
      return leftCols
    }
    if (rightCols.last.isEmpty) {
      return rightCols
    }

    val leftTuples = leftCols.take(leftCols.length - 1).transpose
    val leftActiveTuples =  leftTuples.filter(tuple => leftCols.last(leftTuples.indexOf(tuple)).asInstanceOf[Boolean]).map(tuple => (tuple, getLeftKeys.map(k => tuple(k))))

    val rightTuples = rightCols.take(rightCols.length - 1).transpose
    val rightActiveTuples =  rightTuples.filter(tuple => rightCols.last(rightTuples.indexOf(tuple)).asInstanceOf[Boolean]).map(tuple => (tuple, getRightKeys.map(k => tuple(k))))

    leftActiveTuples.map(leftT => rightActiveTuples.map(rightT => {
      if (leftT._2 == rightT._2) {
        joinTuples = joinTuples :+ (leftT._1.toList ++ rightT._1.toList).toIndexedSeq
      }
    }))
    joinTuples = joinTuples.transpose
    //println("joinTuples: " + joinTuples)
    joinTuples :+ (0 until joinTuples.head.size).map(_ => true)
  }
}
