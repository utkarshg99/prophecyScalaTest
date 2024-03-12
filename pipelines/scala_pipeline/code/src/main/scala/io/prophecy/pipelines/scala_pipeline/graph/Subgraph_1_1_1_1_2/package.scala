package io.prophecy.pipelines.scala_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_1_1_1_2.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1_1_1_1_2 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame = {
    val df_Reformat_1_1_1_1_1_2 = Reformat_1_1_1_1_1_2(context, in0)
    val df_Filter_1_1_1_1_1_2 =
      Filter_1_1_1_1_1_2(context, df_Reformat_1_1_1_1_1_2)
    val df_by_customer_id_and_first_name_1_1_1_1_2 =
      by_customer_id_and_first_name_1_1_1_1_2(context, df_Filter_1_1_1_1_1_2)
    val df_SetOperation_1_1_1_1_1_2 = SetOperation_1_1_1_1_1_2(
      context,
      df_by_customer_id_and_first_name_1_1_1_1_2,
      df_by_customer_id_and_first_name_1_1_1_1_2
    )
    val df_Filter_2_1_1_1_1_2 =
      Filter_2_1_1_1_1_2(context, df_SetOperation_1_1_1_1_1_2)
    val df_by_customer_id_nulls_last_1_1_1_1_2 =
      by_customer_id_nulls_last_1_1_1_1_2(context, df_Filter_2_1_1_1_1_2)
    val df_by_last_name_1_1_1_1_2 =
      by_last_name_1_1_1_1_2(context, df_by_customer_id_nulls_last_1_1_1_1_2)
    val df_Limit_1_1_1_1_1_2 = Limit_1_1_1_1_1_2(context, in1)
    val df_Join1_1_1_1_1_2 =
      Join1_1_1_1_1_2(context, df_Limit_1_1_1_1_1_2, df_Limit_1_1_1_1_1_2)
    val df_Repartition_1_1_1_1_1_2 =
      Repartition_1_1_1_1_1_2(context, df_Join1_1_1_1_1_2)
    val df_select_all_from_in0_1_1_1_1_2 =
      select_all_from_in0_1_1_1_1_2(context, df_Repartition_1_1_1_1_1_2)
    val df_by_customer_id_join_projection_1_1_1_2 =
      by_customer_id_join_projection_1_1_1_2(context,
                                             df_by_last_name_1_1_1_1_2,
                                             df_select_all_from_in0_1_1_1_1_2
      )
    df_by_customer_id_join_projection_1_1_1_2
  }

}
