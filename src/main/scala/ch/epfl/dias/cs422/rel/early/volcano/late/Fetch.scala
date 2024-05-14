package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalFetch
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, LateTuple, NilLateTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
import ch.epfl.dias.cs422.helpers.store.late.LateStandaloneColumnStore
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters.CollectionHasAsScala

class Fetch protected (
                              input: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                              fetchType: RelDataType,
                              column: LateStandaloneColumnStore,
                              projects: Option[java.util.List[_ <: RexNode]],
                            ) extends skeleton.Fetch[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](input, fetchType, column, projects)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  lazy val evaluator: Tuple => Tuple =
    eval(projects.get.asScala.toIndexedSeq, fetchType)

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    val nextLateTuple = input.next()
    nextLateTuple match {
      case NilLateTuple =>
        NilLateTuple
      case Some(tuple) => projects match {
        case None =>
          Option(LateTuple(tuple.vid, tuple.value ++ column.getElement(tuple.vid)))
        case Some(_) =>
          val tup = column.getElement(tuple.vid).toList.toIndexedSeq

          Option(LateTuple(tuple.vid, tuple.value ++ evaluator(tup)))
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}

object Fetch {
  def create(
              input: Operator,
              fetchType: RelDataType,
              column: LateStandaloneColumnStore,
              projects: Option[java.util.List[_ <: RexNode]]
            ): LogicalFetch = {
    new Fetch(input, fetchType, column, projects)
  }
}

