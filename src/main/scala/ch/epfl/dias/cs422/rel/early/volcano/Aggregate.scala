package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import ch.epfl.dias.cs422.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.util.ImmutableBitSet

import scala.+:
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.{`iterable AsScalaIterable`, `map AsJavaMap`, `seq AsJavaList`}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  private var tuples: List[Tuple] = List()
  private var current = 0
  private var rowCount = 0
  private var rowStore: Option[IndexedSeq[IndexedSeq[Elem]]] = None

  @tailrec
  private def fillTuples(tuples: List[Tuple]): List[Tuple] = {
    val nextTuple: Option[Tuple] = input.next()
    nextTuple match {
      case NilTuple => tuples
      case Some(t) => fillTuples(tuples :+ t)
    }
  }

  def updateAggregate(): Unit = {
    if (tuples.isEmpty && groupSet.length() == 0) {
      val emptyGroup: IndexedSeq[IndexedSeq[Elem]] = aggCalls.map(_ => aggCalls.map(agg => aggEmptyValue(agg)))
      //println("emptyGroup: " + emptyGroup)
      rowCount = emptyGroup.length
      rowStore = Option(emptyGroup)
    } else {
      //println("init tuple: " + tuples)
      val groupedTuples =
        tuples.groupBy(tuple => groupSet.map(bit => tuple.get(bit)))
      //println("groupedTuples: " + groupedTuples)

      val aggResult = groupedTuples.map(group =>
        group._1.toIndexedSeq ++ aggCalls.map(agg => group._2.map(tuple => agg.getArgument(tuple)).reduce((e1, e2) => agg.reduce(e1, e2)))
      ).toIndexedSeq
      //println("aggResult: " + aggResult + " aggResult length: " + aggResult.length)

      rowCount = aggResult.length
      rowStore = Option(aggResult)
    }
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    current = 0
    tuples = fillTuples(List())
    updateAggregate()
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
    input.close()
  }
}
