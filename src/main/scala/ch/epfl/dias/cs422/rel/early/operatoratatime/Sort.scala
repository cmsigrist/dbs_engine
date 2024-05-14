package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem}
import org.apache.calcite.rel.RelCollation

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.jdk.CollectionConverters._
import scala.math.Ordered.orderingToOrdered

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    val nextCols = input.execute()

    if (nextCols.last.isEmpty) {
      return nextCols
    }

    val tuples = nextCols.take(nextCols.length - 1).transpose
    val activeTuples = tuples.filter(tuple => nextCols.last(tuples.indexOf(tuple)).asInstanceOf[Boolean])

    val sortedKeys = activeTuples.map(tuple => (tuple, collation.getFieldCollations.map(c =>
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

    sortedTuples.transpose :+ nextCols.last
  }
}
