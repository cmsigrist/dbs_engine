package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.convert.ImplicitConversions.{`iterable AsScalaIterable`, `seq AsJavaList`}
import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
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
  override def execute(): IndexedSeq[Column] = {
    val nextCol = input.execute()
    val tuples = nextCol.take(nextCol.length - 1).transpose
    val activeTuples = tuples.filter(tuple => nextCol.last(tuples.indexOf(tuple)).asInstanceOf[Boolean])
    //println("activeTuple: " + activeTuples)
    //val tuples = nextCol.take(nextCol.length - 1).transpose //Take only active tuples
    if (activeTuples.isEmpty && groupSet.length() == 0) {
      val aggC = aggCalls.map(_ => aggCalls.map(agg => aggEmptyValue(agg)))
       aggC :+ (0 until aggC.head.size).map(_ => true)
    } else {
      val groupedTuples =
        activeTuples.groupBy(tuple => groupSet.map(bit => tuple.get(bit)))
      //println("groupedTuples: " + groupedTuples)

      val aggResult = groupedTuples.map(group =>
        group._1.toIndexedSeq ++ aggCalls.map(agg => group._2.map(tuple => agg.getArgument(tuple)).reduce((e1, e2) => agg.reduce(e1, e2)))
      ).toIndexedSeq.transpose
      //println("aggRes: " + aggResult)
      aggResult :+ (0 until aggResult.head.size).map(_ => true)
    }
  }
}
