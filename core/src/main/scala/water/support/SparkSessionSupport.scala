package water.support

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Simplify usage of SparkSession from application code.
  *
  * Can be mixed only together with SparkContextSupport
  */
trait SparkSessionSupport { self: SparkContextSupport =>

  @transient lazy val spark = SparkSession.builder().getOrCreate()

  @transient lazy val sqlContext = spark.sqlContext
}
