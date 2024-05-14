package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    var joinTuples: IndexedSeq[Column] = Vector()
    val leftCols = left.execute()
    val rightCols = right.execute()

    if (leftCols.last.isEmpty) {
      return leftCols
    }
    if (rightCols.last.isEmpty) {
      return rightCols
    }

    val leftTuples = leftCols.take(leftCols.length - 1).map(col => asIterable(col)).transpose
    val activeLeft = unwrap[Boolean](leftCols.last)
    val leftActiveTuples = leftTuples.filter(tuple => activeLeft(leftTuples.indexOf(tuple))).map(tuple => (tuple, getLeftKeys.map(k => tuple(k))))

    val rightTuples = rightCols.take(rightCols.length - 1).map(col => asIterable(col)).transpose
    val activeRight = unwrap[Boolean](rightCols.last)
    val rightActiveTuples = rightTuples.filter(tuple => activeRight(rightTuples.indexOf(tuple))).map(tuple => (tuple, getRightKeys.map(k => tuple(k))))

    leftActiveTuples.map(leftT => rightActiveTuples.map(rightT => {
      if (leftT._2 == rightT._2) {
        joinTuples = joinTuples :+ (leftT._1.toList ++ rightT._1.toList).toIndexedSeq
      }
    }))
    joinTuples = joinTuples.transpose

    val homogeneousJoinTuples = joinTuples.map(col => toHomogeneousColumn(col))
    //println("joinTuples: " + joinTuples)
      homogeneousJoinTuples :+ toHomogeneousColumn((0 until joinTuples.head.size).map(_ => true))
  }
}
