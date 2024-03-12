package io.prophecy.pipelines.scala_pipeline.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_1.config.{
  Config => Subgraph_1_1_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_1_1.config.{
  Config => Subgraph_1_1_1_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_2.config.{
  Config => Subgraph_1_2_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_2_1.config.{
  Config => Subgraph_1_2_1_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_2_1_2.config.{
  Config => Subgraph_1_2_1_2_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_2_1_1.config.{
  Config => Subgraph_1_2_1_1_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_1_1_1.config.{
  Config => Subgraph_1_1_1_1_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_1_1_1_2.config.{
  Config => Subgraph_1_1_1_1_2_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1_1_1_1_1.config.{
  Config => Subgraph_1_1_1_1_1_Config
}

case class Config(
  var Subgraph_1:       Subgraph_1_Config = Subgraph_1_Config(),
  var Subgraph_1_1:     Subgraph_1_1_Config = Subgraph_1_1_Config(),
  var Subgraph_1_2:     Subgraph_1_2_Config = Subgraph_1_2_Config(),
  var Subgraph_1_2_1:   Subgraph_1_2_1_Config = Subgraph_1_2_1_Config(),
  var Subgraph_1_1_1:   Subgraph_1_1_1_Config = Subgraph_1_1_1_Config(),
  var Subgraph_1_2_1_1: Subgraph_1_2_1_1_Config = Subgraph_1_2_1_1_Config(),
  var Subgraph_1_1_1_1: Subgraph_1_1_1_1_Config = Subgraph_1_1_1_1_Config(),
  var Subgraph_1_1_1_1_1: Subgraph_1_1_1_1_1_Config =
    Subgraph_1_1_1_1_1_Config(),
  var Subgraph_1_2_1_2: Subgraph_1_2_1_2_Config = Subgraph_1_2_1_2_Config(),
  var Subgraph_1_1_1_1_2: Subgraph_1_1_1_1_2_Config =
    Subgraph_1_1_1_1_2_Config()
) extends ConfigBase
