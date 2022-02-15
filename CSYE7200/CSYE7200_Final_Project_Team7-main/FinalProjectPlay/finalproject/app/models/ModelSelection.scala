package models

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

/**
 * This object is to select best model.
 */
object
ModelSelection {

  /**
   * Key-value map to store all ML models and its accuracy
   */
  val accuracyMap = Map("DT" -> DT.acc, "FM" -> FM.acc, "GBT" -> GBT.acc, "LR" -> LR.acc, "MLP" -> MLP.acc, "RF" -> RF.acc, "SVM" -> SVM.acc)

  /**
   * Find the key with highest accuracy
   */
  val maxAccuracyKey: String = accuracyMap.maxBy(_._2) match {
    case (k, v) => k
  }

  /**
   * Find the best model
   *
   * @param key key in the map above
   * @return the best model
   */
  def findBestModel(key: String) = key match {
    case "DT" => DT.dt
    case "FM" => FM.fm
    case "GBT" => GBT.gbt
    case "LR" => LR.lr
    case "MLP" => MLP.mlp
    case "RF" => RF.rf
    case "SVM" => SVM.svm
  }

  /**
   * Create pipeline for best model
   */
  val pipeline: Pipeline = new Pipeline().setStages(Array(findBestModel(maxAccuracyKey)))

  val bestModel: PipelineModel = pipeline.fit(ProcessData.assTrain)

  /**
   * Predict testing dataset with best model
   */
  val testPred: DataFrame = bestModel.transform(ProcessData.assTest)

  /**
   * Computing testing accuracy with best model
   */
  val testAccuracy: Double = Evaluator.evaluator_binary.evaluate(testPred)

  /**
   * Save best model locally
   */
  bestModel.write.overwrite().save("./bestModel")

  /**
   * Load best model from local
   */
  val loadModel: PipelineModel = PipelineModel.load("./bestModel")

  //println(testAccuracy)
  //println(maxAccuracyKey)
}
