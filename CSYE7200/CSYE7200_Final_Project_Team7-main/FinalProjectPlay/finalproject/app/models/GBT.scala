package models

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.sql.DataFrame

/**
 * This object is Gradient-boosted Tree model.
 */
object GBT {

  /**
   * Set parameters for gradient-boosted tree
   */
  val gbt: GBTClassifier = new GBTClassifier()
    .setLabelCol("Result")
    .setFeaturesCol("features")
    .setMaxIter(10)
    .setFeatureSubsetStrategy("auto")

  /**
   * Fit gradient-boosted tree with training dataset
   */
  val gbtModel: GBTClassificationModel = gbt.fit(ProcessData.assTrain)

  /**
   * Predict validation dataset with gradient-boosted tree
   */
  val validPred: DataFrame = gbtModel.transform(ProcessData.assValid)

  /**
   * Compute accuracy for validation dataset
   */
  val acc: Double = Evaluator.evaluator_binary.evaluate(validPred)

  //println(acc)
}
