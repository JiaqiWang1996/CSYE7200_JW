package models

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import play.api.data.Form
import play.api.data.Forms._

import java.util.Date

case class Main(tmd: Int, tpd: Int, jnd: Int, mdd: Int, acd: Int, spd: Int, dg: Int, stu: Int, kil: Int)


/**
 * This object is for user interactive and loading model
 */
object Main {

  val form = Form(
    mapping(
      "tmd" -> number,
      "tpd" -> number,
      "jnd" -> number,
      "mdd" -> number,
      "acd" -> number,
      "spd" -> number,
      "dg" -> number,
      "stu" -> number,
      "kil" -> number
    )(Main.apply)(Main.unapply)
  )

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
  val bestModel = {
    if (file.exists) {
      PipelineModel.load(filePath)
    }
    else {
      ModelSelection.loadModel
    }
  }

  /**
   * User interactive system allows to input required data
   */


  def predict(main: Main): String = {
    /**
     * Timestamp to record the start time of predicting system
     */
    val prev = new Date()

    /**
     * Convert user inputs into required dataframe
     */
    val df = ProcessData.spark.createDataFrame(Seq(
      (main.tmd, main.tpd, main.jnd, main.mdd, main.acd, main.spd, main.dg, main.stu, main.kil),
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

    val ans = {
      if (finalRes == 1.0) "You will win!!!"
      else "You will lose."
    }
    ans
  }


  //  ProcessData.spark.close()

}
