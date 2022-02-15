import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler

import java.util.Date
import scala.util.control.Breaks.{break, breakable}

/**
 * This object is for user interactive and loading model
 */
object Main extends App {
  ProcessData.spark
  /**
   * Find file dictionary of saved best model
   */
  val filePath = "./bestModel"

  val file = scala.reflect.io.File(filePath)

  /**
   * If best model was saved, just loading it
   * Or automatically fitting different models, finding the best model, saving and loading it
   */
  val bestModel: PipelineModel = {
    if (file.exists) {
      PipelineModel.load(filePath)
    } else {
      ModelSelection.loadModel
    }
  }

  /**
   * User interactive system allows to input required data
   */
  breakable {
    while (true) {
      println("Press 1 to predict, else will close the app.")
      val condition = scala.io.StdIn.readInt()
      if (condition == 1) {
        println("Please input Team Gold Diff:")
        val tmd = scala.io.StdIn.readInt()

        println("Please input Top Gold Diff:")
        val tpd = scala.io.StdIn.readInt()

        println("Please input Jungle Gold Diff:")
        val jnd = scala.io.StdIn.readInt()

        println("Please input Mid Gold Diff:")
        val mdd = scala.io.StdIn.readInt()

        println("Please input ADC Gold Diff:")
        val acd = scala.io.StdIn.readInt()

        println("Please input Support Gold Diff:")
        val spd = scala.io.StdIn.readInt()

        println("Please input Dragon you got:")
        val dg = scala.io.StdIn.readInt()

        println("Please input Structures you got:")
        val stu = scala.io.StdIn.readInt()

        println("Please input kills you got:")
        val kil = scala.io.StdIn.readInt()

        /**
         * Timestamp to record the start time of predicting system
         */
        val prev = new Date()

        /**
         * Convert user inputs into required dataframe
         */
        val df = ProcessData.spark.createDataFrame(Seq(
          (tmd, tpd, jnd, mdd, acd, spd, dg, stu, kil),
        )).toDF("TeamDiff", "TopDiff", "JunDiff", "MidDiff", "ADCDiff", "SupDiff", "Dragons", "Structures", "Kills")

        val assembler = new VectorAssembler()
          .setInputCols(Array("TeamDiff", "TopDiff", "JunDiff", "MidDiff", "ADCDiff", "SupDiff", "Dragons", "Structures", "Kills"))
          .setOutputCol("features")

        val validData = assembler.transform(df)

        /**
         * Predict user inputs with selected best model
         */
        val predictions = bestModel.transform(validData)
        predictions.show(false)

        /**
         * Timestamp to record the end time of predicting system
         */
        val now = new Date()

        /**
         * Show Predicted results including response time and final result
         */
        println("Pridict Time: " + ((now.getTime - prev.getTime).toDouble / 1000))

        val finalRes = predictions.select("prediction").rdd.first().getDouble(0)

        if (finalRes == 1.0) {
          println("You will win!")
        } else {
          println("You will lose!")
        }
      } else {
        break
      }
    }
  }

  ProcessData.spark.close()
}
