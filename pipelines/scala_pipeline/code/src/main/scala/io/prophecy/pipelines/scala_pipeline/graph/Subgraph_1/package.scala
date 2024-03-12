package io.prophecy.pipelines.scala_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1.config._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1.Subgraph_1_3
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): Subgraph2 = {
    val df_Reformat_1_1 = Reformat_1_1(context, in0)
    val df_Filter_1_1   = Filter_1_1(context,   df_Reformat_1_1)
    val df_by_customer_id_and_first_name_1 =
      by_customer_id_and_first_name_1(context, df_Filter_1_1)
    val df_SetOperation_1_1 = SetOperation_1_1(
      context,
      df_by_customer_id_and_first_name_1,
      df_by_customer_id_and_first_name_1
    )
    val df_Filter_2_1 = Filter_2_1(context, df_SetOperation_1_1)
    val df_by_customer_id_nulls_last_1 =
      by_customer_id_nulls_last_1(context, df_Filter_2_1)
    val df_by_last_name_1 =
      by_last_name_1(context, df_by_customer_id_nulls_last_1)
    val (df_Subgraph_1_3_out0, df_Subgraph_1_3_out1) = Subgraph_1_3.apply(
      Subgraph_1_3.config.Context(context.spark, context.config.Subgraph_1_3),
      df_by_last_name_1,
      df_by_last_name_1
    )
    val df_Limit_1_1       = Limit_1_1(context,       in1)
    val df_Join1_1         = Join1_1(context,         df_Limit_1_1, df_Limit_1_1)
    val df_Repartition_1_1 = Repartition_1_1(context, df_Join1_1)
    val df_select_all_from_in0_1 =
      select_all_from_in0_1(context, df_Repartition_1_1)
    val df_by_customer_id_join_projection = by_customer_id_join_projection(
      context,
      df_by_last_name_1,
      df_select_all_from_in0_1
    )
    (df_by_customer_id_join_projection, df_select_all_from_in0_1)
  }

}
