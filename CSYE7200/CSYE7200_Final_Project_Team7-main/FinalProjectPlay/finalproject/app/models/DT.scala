package models

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.sql.DataFrame

/**
 * This object is Decision Tree model.
 */
object DT {

  /**
   * Set parameters for decision tree
   */
  val dt: DecisionTreeClassifier = new DecisionTreeClassifier()
    .setLabelCol("Result")
    .setFeaturesCol("features")
    .setMinInstancesPerNode(25)

  /**
   * Fit decision tree with training dataset
   */
  val dtModel: DecisionTreeClassificationModel = dt.fit(ProcessData.assTrain)

  /**
   * Predict validation dataset with decision tree
   */
  val validPred: DataFrame = dtModel.transform(ProcessData.assValid)

  /**
   * Compute accuracy for validation dataset
   */
  val acc: Double = Evaluator.evaluator_binary.evaluate(validPred)

  //println(acc)
}
