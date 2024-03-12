package io.prophecy.pipelines.scala_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_pipeline.udfs.UDFs._
import io.prophecy.pipelines.scala_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object by_last_name {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("last_name"))
      .agg(first(col("customer_id")).as("customer_id"),
           first(col("first_name")).as("first_name")
      )

}
