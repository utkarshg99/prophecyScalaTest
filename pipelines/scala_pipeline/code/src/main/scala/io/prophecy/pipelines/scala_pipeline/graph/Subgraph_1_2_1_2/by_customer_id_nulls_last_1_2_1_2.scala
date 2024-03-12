package io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_2_1_2

import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_pipeline.udfs.UDFs._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_2_1_2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object by_customer_id_nulls_last_1_2_1_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("customer_id").asc_nulls_last)

}
