package io.prophecy.pipelines.scala_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.config._
import io.prophecy.pipelines.scala_pipeline.udfs.UDFs._
import io.prophecy.pipelines.scala_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_pipeline.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_dataset_cust_in = dataset_cust_in(context)
    val df_Reformat_1      = Reformat_1(context, df_dataset_cust_in)
    val df_Filter_1        = Filter_1(context,   df_Reformat_1)
    val df_by_customer_id_and_first_name =
      by_customer_id_and_first_name(context, df_Filter_1)
    val df_SetOperation_1 = SetOperation_1(context,
                                           df_by_customer_id_and_first_name,
                                           df_by_customer_id_and_first_name
    )
    val df_Filter_2 = Filter_2(context, df_SetOperation_1)
    val df_by_customer_id_nulls_last =
      by_customer_id_nulls_last(context, df_Filter_2)
    val df_by_last_name = by_last_name(context, df_by_customer_id_nulls_last)
    val df_SetOperation_1_2 =
      SetOperation_1_2(context, df_by_last_name, df_by_last_name)
    val df_Filter_2_2    = Filter_2_2(context,    df_SetOperation_1_2)
    val df_Limit_1       = Limit_1(context,       df_dataset_cust_in)
    val df_Join1         = Join1(context,         df_Limit_1, df_Limit_1)
    val df_Repartition_1 = Repartition_1(context, df_Join1)
    val df_select_all_from_in0_3 =
      select_all_from_in0_3(context, df_Repartition_1)
    val df_Limit_1_2_1 = Limit_1_2_1(context, df_select_all_from_in0_3)
    val df_select_all_from_in0_3_1 =
      select_all_from_in0_3_1(context, df_Repartition_1)
    val df_Limit_1_2_1_1 = Limit_1_2_1_1(context, df_select_all_from_in0_3_1)
    val df_Join1_2_1_1 =
      Join1_2_1_1(context, df_Limit_1_2_1_1, df_Limit_1_2_1_1)
    val df_Repartition_1_2_1_1 = Repartition_1_2_1_1(context, df_Join1_2_1_1)
    val df_select_all_from_in0_2_1_1 =
      select_all_from_in0_2_1_1(context, df_Repartition_1_2_1_1)
    val df_select_all_from_in0_8 =
      select_all_from_in0_8(context, df_select_all_from_in0_2_1_1)
    val df_Limit_1_2_6 = Limit_1_2_6(context, df_select_all_from_in0_8)
    val df_Join1_2_6   = Join1_2_6(context,   df_Limit_1_2_6, df_Limit_1_2_6)
    val df_select_all_from_in0_4 =
      select_all_from_in0_4(context, df_Repartition_1)
    val df_Limit_1_2_2       = Limit_1_2_2(context,       df_select_all_from_in0_4)
    val df_Join1_2_2         = Join1_2_2(context,         df_Limit_1_2_2, df_Limit_1_2_2)
    val df_Repartition_1_2_2 = Repartition_1_2_2(context, df_Join1_2_2)
    val df_select_all_from_in0_2_2 =
      select_all_from_in0_2_2(context, df_Repartition_1_2_2)
    val df_select_all_from_in0_7 =
      select_all_from_in0_7(context, df_select_all_from_in0_2_2)
    val df_Limit_1_2_5         = Limit_1_2_5(context,         df_select_all_from_in0_7)
    val df_Join1_2_5           = Join1_2_5(context,           df_Limit_1_2_5, df_Limit_1_2_5)
    val df_select_all_from_in0 = select_all_from_in0(context, df_Repartition_1)
    val df_Limit_1_2           = Limit_1_2(context,           df_select_all_from_in0)
    val df_Join1_2             = Join1_2(context,             df_Limit_1_2,   df_Limit_1_2)
    val df_Repartition_1_2     = Repartition_1_2(context,     df_Join1_2)
    val df_select_all_from_in0_2 =
      select_all_from_in0_2(context, df_Repartition_1_2)
    val df_select_all_from_in0_5 =
      select_all_from_in0_5(context, df_select_all_from_in0_2)
    val df_Limit_1_2_3 = Limit_1_2_3(context, df_select_all_from_in0_5)
    val df_by_last_name_3 =
      by_last_name_3(context, df_by_customer_id_nulls_last)
    val df_SetOperation_1_2_1_1 =
      SetOperation_1_2_1_1(context, df_by_last_name_3, df_by_last_name_3)
    val df_Filter_2_2_1_1 = Filter_2_2_1_1(context, df_SetOperation_1_2_1_1)
    val df_by_customer_id_nulls_last_2_1_1 =
      by_customer_id_nulls_last_2_1_1(context, df_Filter_2_2_1_1)
    val df_by_last_name_2_1_1 =
      by_last_name_2_1_1(context, df_by_customer_id_nulls_last_2_1_1)
    val df_SetOperation_1_2_1 =
      SetOperation_1_2_1(context, df_by_last_name, df_by_last_name)
    val df_Filter_2_2_1 = Filter_2_2_1(context, df_SetOperation_1_2_1)
    val df_by_customer_id_nulls_last_2_1 =
      by_customer_id_nulls_last_2_1(context, df_Filter_2_2_1)
    val df_Subgraph_1_1 = Subgraph_1_1.apply(
      Subgraph_1_1.config.Context(context.spark, context.config.Subgraph_1_1),
      df_dataset_cust_in,
      df_dataset_cust_in
    )
    val df_Subgraph_1_1_1 = Subgraph_1_1_1.apply(
      Subgraph_1_1_1.config
        .Context(context.spark, context.config.Subgraph_1_1_1),
      df_Subgraph_1_1,
      df_Subgraph_1_1
    )
    val df_Join1_2_1         = Join1_2_1(context,         df_Limit_1_2_1, df_Limit_1_2_1)
    val df_Repartition_1_2_1 = Repartition_1_2_1(context, df_Join1_2_1)
    val df_select_all_from_in0_2_1 =
      select_all_from_in0_2_1(context, df_Repartition_1_2_1)
    val df_select_all_from_in0_6 =
      select_all_from_in0_6(context, df_select_all_from_in0_2_1)
    val df_Limit_1_2_4       = Limit_1_2_4(context,       df_select_all_from_in0_6)
    val df_Join1_2_4         = Join1_2_4(context,         df_Limit_1_2_4, df_Limit_1_2_4)
    val df_Repartition_1_2_4 = Repartition_1_2_4(context, df_Join1_2_4)
    val df_select_all_from_in0_2_4 =
      select_all_from_in0_2_4(context, df_Repartition_1_2_4)
    val df_by_last_name_2_1 =
      by_last_name_2_1(context, df_by_customer_id_nulls_last_2_1)
    val df_Reformat_3        = Reformat_3(context,        df_by_last_name_2_1)
    val df_Reformat_3_1      = Reformat_3_1(context,      df_Reformat_3)
    val df_Reformat_3_2      = Reformat_3_2(context,      df_Reformat_3_1)
    val df_Reformat_3_1_1    = Reformat_3_1_1(context,    df_Reformat_3_2)
    val df_Join1_2_3         = Join1_2_3(context,         df_Limit_1_2_3, df_Limit_1_2_3)
    val df_Repartition_1_2_3 = Repartition_1_2_3(context, df_Join1_2_3)
    val df_select_all_from_in0_2_3 =
      select_all_from_in0_2_3(context, df_Repartition_1_2_3)
    val df_by_customer_id_nulls_last_2 =
      by_customer_id_nulls_last_2(context, df_Filter_2_2)
    val df_by_last_name_2 =
      by_last_name_2(context, df_by_customer_id_nulls_last_2)
    val df_Reformat_4        = Reformat_4(context,        df_by_last_name_2)
    val df_Reformat_2        = Reformat_2(context,        df_by_last_name_2_1_1)
    val df_Reformat_2_1      = Reformat_2_1(context,      df_Reformat_2)
    val df_Reformat_2_2      = Reformat_2_2(context,      df_Reformat_2_1)
    val df_Reformat_2_1_1    = Reformat_2_1_1(context,    df_Reformat_2_2)
    val df_Repartition_1_2_5 = Repartition_1_2_5(context, df_Join1_2_5)
    val df_select_all_from_in0_2_5 =
      select_all_from_in0_2_5(context, df_Repartition_1_2_5)
    val (df_Subgraph_1_out0, df_Subgraph_1_out1) = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1),
      df_dataset_cust_in,
      df_dataset_cust_in
    )
    val (df_Subgraph_1_2_out0, df_Subgraph_1_2_out1) = Subgraph_1_2.apply(
      Subgraph_1_2.config.Context(context.spark, context.config.Subgraph_1_2),
      df_Subgraph_1_out0,
      df_Subgraph_1_out1
    )
    val (df_Subgraph_1_2_1_out0, df_Subgraph_1_2_1_out1) = Subgraph_1_2_1.apply(
      Subgraph_1_2_1.config
        .Context(context.spark, context.config.Subgraph_1_2_1),
      df_Subgraph_1_2_out0,
      df_Subgraph_1_2_out1
    )
    val (df_Subgraph_1_2_1_2_out0, df_Subgraph_1_2_1_2_out1) =
      Subgraph_1_2_1_2.apply(
        Subgraph_1_2_1_2.config
          .Context(context.spark, context.config.Subgraph_1_2_1_2),
        df_Subgraph_1_2_1_out0,
        df_Subgraph_1_2_1_out0
      )
    val (df_Subgraph_1_2_1_1_out0, df_Subgraph_1_2_1_1_out1) =
      Subgraph_1_2_1_1.apply(
        Subgraph_1_2_1_1.config
          .Context(context.spark, context.config.Subgraph_1_2_1_1),
        df_Subgraph_1_2_1_out1,
        df_Subgraph_1_2_1_out0
      )
    val df_Subgraph_1_1_1_1 = Subgraph_1_1_1_1.apply(
      Subgraph_1_1_1_1.config
        .Context(context.spark, context.config.Subgraph_1_1_1_1),
      df_Subgraph_1_1,
      df_Subgraph_1_1
    )
    val df_Subgraph_1_1_1_1_2 = Subgraph_1_1_1_1_2.apply(
      Subgraph_1_1_1_1_2.config
        .Context(context.spark, context.config.Subgraph_1_1_1_1_2),
      df_Subgraph_1_1,
      df_Subgraph_1_1
    )
    val df_Repartition_1_2_6 = Repartition_1_2_6(context, df_Join1_2_6)
    val df_select_all_from_in0_2_6 =
      select_all_from_in0_2_6(context, df_Repartition_1_2_6)
    val df_Subgraph_1_1_1_1_1 = Subgraph_1_1_1_1_1.apply(
      Subgraph_1_1_1_1_1.config
        .Context(context.spark, context.config.Subgraph_1_1_1_1_1),
      df_Subgraph_1_1,
      df_Subgraph_1_1
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/scala_pipeline")
    registerUDFs(spark)
    try MetricsCollector.start(spark,
                               "pipelines/scala_pipeline",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/scala_pipeline")
    }
    apply(context)
    MetricsCollector.end(spark)
  }

}
