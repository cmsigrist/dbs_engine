package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode

import scala.annotation.tailrec

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  private var rowStore: Option[List[Tuple]] = None
  private var current = 0
  private var rowCount = 0

  def computeJoin(left: List[Tuple], right: List[Tuple]): List[Tuple] = {
    var joinTuples: List[Tuple] = List()
    val leftTuples = left.map(tuple => (tuple, getLeftKeys.map(k => tuple(k))))
    val rightTuples = right.map(tuple => (tuple, getRightKeys.map(k => tuple(k))))

    leftTuples.map(leftT => rightTuples.map(rightT => {
      if (leftT._2 == rightT._2) {
        joinTuples = joinTuples :+ (leftT._1.toList ++ rightT._1.toList).toIndexedSeq
      }
    }))
    joinTuples
  }

  @tailrec
  private def fillTuples(tuples: List[Tuple], input: Operator): List[Tuple] = {
    val nextTuple: Option[Tuple] = input.next()
    nextTuple match {
      case NilTuple => tuples
      case Some(_) => fillTuples(tuples :+ nextTuple.get, input)
    }
  }
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    val leftTuples = fillTuples(List(), left)
    right.open()
    val rightTuples = fillTuples(List(), right)

    val joinTuples = computeJoin(leftTuples, rightTuples)
    rowStore = Option(joinTuples)
    rowCount = joinTuples.length
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (current == rowCount) {
      NilTuple
    } else {
      val nextTuple = Option(rowStore.get(current))
      current += 1
      nextTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
