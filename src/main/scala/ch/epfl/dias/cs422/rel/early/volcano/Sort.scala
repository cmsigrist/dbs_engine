package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.math.Ordered.orderingToOrdered

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  private var tuples: List[Tuple] = List()
  private var current = 0
  private var rowCount = 0
  private var rowStore: Option[List[Tuple]] = None

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
    input.open()
    current = 0
    tuples = fillTuples(List() , input)

    val sortedKeys = tuples.map(tuple => (tuple, collation.getFieldCollations.map(c =>
      (tuple(c.getFieldIndex), c.direction)).toList))

    val sortedTuples =
      tuples.sortBy(tuple => sortedKeys.find(sk => sk._1.equals(tuple)).get)((tuple1, tuple2) => {
        var result = 0
        var i = 0
        while ( i < collation.getFieldCollations.size() && result == 0) {
          val t1 = tuple1._2(i)._1.asInstanceOf[Comparable[Elem]]
          val t2 = tuple2._2(i)._1.asInstanceOf[Comparable[Elem]]
          if (tuple1._2(i)._2.isDescending) {
            result = t2 compare t1
          } else {
            result = t1 compare t2
          }
          i += 1
        }
        result
      })

    rowStore = Option(sortedTuples)
    rowCount = sortedTuples.length

    if (offset.nonEmpty) {
      current += offset.get
    }
  }


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (current == rowCount) {
      NilTuple
    } else {
      if (fetch.nonEmpty) {
        if(current == fetch.get) {
          return NilTuple
        }
      }
      val nextTuple = Option(rowStore.get(current))
      current += 1
      nextTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
