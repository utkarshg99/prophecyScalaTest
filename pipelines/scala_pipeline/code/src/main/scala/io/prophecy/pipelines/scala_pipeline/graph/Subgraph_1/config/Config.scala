package io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1.Subgraph_1_3.config.{
  Config => Subgraph_1_3_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(var Subgraph_1_3: Subgraph_1_3_Config = Subgraph_1_3_Config())
    extends ConfigBase

case class Context(spark: SparkSession, config: Config)
