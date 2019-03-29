package com.cloudera.education.spark.udfs

import org.apache.spark.sql.api.java.UDF1
import scala.math.log
import scala.math.exp
 
class Logit extends UDF1[Double, Double] {
  override def call(x: Double): Double = log(x / (1.0 - x))
}

class Expit extends UDF1[Double, Double] {
  override def call(x: Double): Double = 1.0 / (1.0 + exp(-x))
}
