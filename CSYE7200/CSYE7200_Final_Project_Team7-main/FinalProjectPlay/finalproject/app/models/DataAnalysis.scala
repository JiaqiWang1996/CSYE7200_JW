package models

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions.monotonically_increasing_id

/**
 * This object is to do data analysis.
 */
object DataAnalysis extends App {

  /**
   * Read and convert data.
   */
  val rddpreD = ProcessData.predData.rdd.map { row =>
    val first = row.getAs[Integer]("Result")
    val second = row.getAs[Integer]("TeamDiff")
    val third = row.getAs[Integer]("TopDiff")
    val fourth = row.getAs[Integer]("JunDiff")
    val fifth = row.getAs[Integer]("MidDiff")
    val sixth = row.getAs[Integer]("ADCDiff")
    val seventh = row.getAs[Integer]("SupDiff")
    val eighth = row.getAs[Integer]("Dragons")
    val ninth = row.getAs[Integer]("Structures")
    val tenth = row.getAs[Integer]("Kills")
    Vectors.dense(first.toDouble, second.toDouble, third.toDouble, fourth.toDouble, fifth.toDouble, sixth.toDouble, seventh.toDouble, eighth.toDouble, ninth.toDouble, tenth.toDouble)
  }

  /**
   * Compute correlation.
   */
  val correlMatrix = Statistics.corr(rddpreD)


  /**
   * Convert correlation matrix to dataframe
   */

  import ProcessData.spark.implicits._

  val cols = (0 until correlMatrix.numCols)

  val df = correlMatrix.transpose
    .colIter.toSeq
    .map(_.toArray)
    .toDF("arr")

  val col = List("Result", "TeamDiff", "TopDiff", "JunDiff", "MidDiff", "ADCDiff", "SupDiff", "Dragons", "Structures", "Kills").toDF("Cor-Matrix")
    .withColumn("id1", monotonically_increasing_id())

  val cor = cols.foldLeft(df)((df, i) => df.withColumn("_" + (i + 1), $"arr"(i)))
    .drop("arr")
    .withColumnRenamed("_1", "Result")
    .withColumnRenamed("_2", "TeamDiff")
    .withColumnRenamed("_3", "TopDiff")
    .withColumnRenamed("_4", "JunDiff")
    .withColumnRenamed("_5", "MidDiff")
    .withColumnRenamed("_6", "ADCDiff")
    .withColumnRenamed("_7", "SupDiff")
    .withColumnRenamed("_8", "Dragons")
    .withColumnRenamed("_9", "Structures")
    .withColumnRenamed("_10", "Kills")
    .withColumn("id2", monotonically_increasing_id())

  col.join(cor, col("id1") === cor("id2"), "inner")
    .drop("id1")
    .drop("id2")
    .show(false)
}
