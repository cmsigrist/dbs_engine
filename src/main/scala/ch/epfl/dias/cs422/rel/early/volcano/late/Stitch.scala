package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple}
import ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator

import scala.annotation.tailrec

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Stitch]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Stitch protected(
                              left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                              right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
                            ) extends skeleton.Stitch[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

  @tailrec
  private def fillTuples(tuples: List[LateTuple], input: Operator): List[LateTuple] = {
    val nextTuple: Option[LateTuple] = input.next()
    nextTuple match {
      case NilLateTuple => tuples
      case Some(_) => fillTuples(tuples :+ nextTuple.get, input)
    }
  }

  var leftTuples: List[LateTuple] = List()
  var rightTuples: List[LateTuple] = List()

  var stitchedTuples: List[LateTuple] = List()
  var rowCount = 0
  var current = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    current = 0
    left.open()
    leftTuples = fillTuples(List(), left)
    right.open()
    rightTuples = fillTuples(List(), right)

    stitchedTuples = for {
      leftT <- leftTuples
      rightT <- rightTuples
      if leftT.vid == rightT.vid
    } yield LateTuple(leftT.vid, leftT.value ++ rightT.value)

    rowCount = stitchedTuples.length
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    if (current == rowCount) {
      NilLateTuple
    } else {
      val nextTuple = stitchedTuples(current)
      current += 1
      Option(nextTuple)
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
