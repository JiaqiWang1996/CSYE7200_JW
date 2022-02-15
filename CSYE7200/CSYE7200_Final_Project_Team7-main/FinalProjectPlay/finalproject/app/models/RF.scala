package models

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.DataFrame

/**
 * This object is Random Forest model.
 */
object RF {

  /**
   * Set parameters for random forest
   */
  val rf: RandomForestClassifier = new RandomForestClassifier()
    .setLabelCol("Result")
    .setFeaturesCol("features")
    .setNumTrees(64)
    .setSeed(3333L)

  /**
   * Fit random forest with multilayer perceptron
   */
  val rfModel: RandomForestClassificationModel = rf.fit(ProcessData.assTrain)

  /**
   * Predict validation dataset with random forest
   */
  val validPred: DataFrame = rfModel.transform(ProcessData.assValid)

  /**
   * Compute accuracy for validation dataset
   */
  val acc: Double = Evaluator.evaluator_binary.evaluate(validPred)

  //println(acc)
}
