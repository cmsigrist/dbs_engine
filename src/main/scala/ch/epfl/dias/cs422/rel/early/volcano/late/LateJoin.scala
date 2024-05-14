package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple}
import ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
import org.apache.calcite.rex.RexNode

import scala.annotation.tailrec

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateJoin(
               left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               condition: RexNode
             ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  private var rowStore: Option[List[LateTuple]] = None
  private var current = 0
  private var rowCount = 0

  def computeJoin(left: List[LateTuple], right: List[LateTuple]): List[LateTuple] = {
    var joinTuples: List[LateTuple] = List()
    val leftTuples = left.map(tuple => (tuple, getLeftKeys.map(k => tuple.value(k))))
    val rightTuples = right.map(tuple => (tuple, getRightKeys.map(k => tuple.value(k))))

    leftTuples.map(leftT => rightTuples.map(rightT => {
      if (leftT._2 == rightT._2) {
        joinTuples = joinTuples :+ LateTuple(leftT._1.vid, leftT._1.value ++ rightT._1.value)
      }
    }))
    joinTuples
  }

  @tailrec
  private def fillTuples(tuples: List[LateTuple], input: Operator): List[LateTuple] = {
    val nextTuple: Option[LateTuple] = input.next()
    nextTuple match {
      case NilLateTuple => tuples
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
  override def next(): Option[LateTuple] = {
    if (current == rowCount) {
      NilLateTuple
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
