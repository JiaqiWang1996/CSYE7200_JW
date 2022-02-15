package models

import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.sql.DataFrame

/**
 * This object is Support Vector Machine model.
 */
object SVM {

  /**
   * Set parameters for support vector machine
   */
  val svm: LinearSVC = new LinearSVC()
    .setMaxIter(10)
    .setRegParam(0)
    .setLabelCol("Result")
    .setFeaturesCol("features")

  /**
   * Fit random forest with support vector machine
   */
  val svmModel: LinearSVCModel = svm.fit(ProcessData.assTrain)

  /**
   * Predict validation dataset with support vector machine
   */
  val validPred: DataFrame = svmModel.transform(ProcessData.assValid)

  /**
   * Compute accuracy for validation dataset
   */
  val acc: Double = Evaluator.evaluator_binary.evaluate(validPred)

  //println(acc)
}
