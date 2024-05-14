package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.convert.ImplicitConversions.{`iterable AsScalaIterable`, `seq AsJavaList`}
import scala.{:+, ::}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val nextCol = input.execute()
    val tuples = nextCol.take(nextCol.length - 1).map(col => asIterable(col)).transpose
    val active = unwrap[Boolean](nextCol.last)
    val activeTuples = tuples.filter(tuple => active(tuples.indexOf(tuple)))

    if (activeTuples.isEmpty && groupSet.length() == 0) {
      val aggC = aggCalls.map(_ => toHomogeneousColumn(aggCalls.map(agg => aggEmptyValue(agg))))
      aggC :+ toHomogeneousColumn((0 until aggC.head.size).map(_ => true))
    } else {
      val groupedTuples =
        activeTuples.groupBy(tuple => groupSet.map(bit => tuple.get(bit)))

      val aggResult = groupedTuples.map(group =>
        group._1.toIndexedSeq ++ aggCalls.map(agg => group._2.map(tuple => agg.getArgument(tuple)).reduce((e1, e2) => agg.reduce(e1, e2)))
      ).toIndexedSeq.transpose.map(col => toHomogeneousColumn(col))
      aggResult :+ toHomogeneousColumn((0 until aggResult.head.size).map(_ => true))
    }
  }
}
