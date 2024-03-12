package io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1

import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1.Subgraph_1_3.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1_3 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): Subgraph2 = {
    val df_Limit_1_1_3    = Limit_1_1_3(context,    in1)
    val df_Join1_13       = Join1_13(context,       df_Limit_1_1_3, df_Limit_1_1_3)
    val df_Reformat_1_1_3 = Reformat_1_1_3(context, in0)
    val df_Filter_1_1_3   = Filter_1_1_3(context,   df_Reformat_1_1_3)
    val df_by_customer_id_and_first_name_1_3 =
      by_customer_id_and_first_name_1_3(context, df_Filter_1_1_3)
    val df_SetOperation_1_1_3 = SetOperation_1_1_3(
      context,
      df_by_customer_id_and_first_name_1_3,
      df_by_customer_id_and_first_name_1_3
    )
    val df_Filter_2_1_3 = Filter_2_1_3(context, df_SetOperation_1_1_3)
    val df_by_customer_id_nulls_last_1_3 =
      by_customer_id_nulls_last_1_3(context, df_Filter_2_1_3)
    val df_by_last_name_1_3 =
      by_last_name_1_3(context, df_by_customer_id_nulls_last_1_3)
    val df_Repartition_1_1_3 = Repartition_1_1_3(context, df_Join1_13)
    val df_select_all_from_in0_1_3 =
      select_all_from_in0_1_3(context, df_Repartition_1_1_3)
    val df_by_customer_id_join_projection_3 = by_customer_id_join_projection_3(
      context,
      df_by_last_name_1_3,
      df_select_all_from_in0_1_3
    )
    (df_by_customer_id_join_projection_3, df_select_all_from_in0_1_3)
  }

}
