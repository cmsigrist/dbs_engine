package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rex.RexBuilder

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  private var prog = getRowType.getFieldList.asScala.map(_ => 0)
  private var current = 0
  private val rowStore: RowStore = scannable.asInstanceOf[RowStore]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    current = 0
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (current == rowStore.getRowCount) {
      NilTuple
    } else {
      val nextTuple: Option[Tuple] = Option(rowStore.getRow(current))
      //println("Scan current: "+ current + " next: " + nextTuple)
      current += 1
      nextTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    current = 0
  }
}
