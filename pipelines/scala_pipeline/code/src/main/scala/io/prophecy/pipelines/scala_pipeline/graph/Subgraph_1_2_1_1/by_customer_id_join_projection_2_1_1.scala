package io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_2_1_1

import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_pipeline.udfs.UDFs._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_2_1_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object by_customer_id_join_projection_2_1_1 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.customer_id") === col("in1.customer_id"),
            "inner"
      )
      .select(
        col("in1.customer_id").as("customer_id"),
        col("in1.first_name").as("first_name"),
        col("in1.last_name").as("last_name"),
        col("in1.phone").as("phone"),
        col("in1.email").as("email"),
        col("in1.country_code").as("country_code"),
        col("in1.account_open_date").as("account_open_date"),
        col("in1.account_flags").as("account_flags")
      )

}
