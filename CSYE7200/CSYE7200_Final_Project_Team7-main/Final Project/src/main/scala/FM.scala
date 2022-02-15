import org.apache.spark.ml.classification.{FMClassificationModel, FMClassifier}
import org.apache.spark.sql.DataFrame

/**
 * This object is Factorization Machines model.
 */
object FM {

  /**
   * Set parameters for factorization machines
   */
  val fm: FMClassifier = new FMClassifier()
    .setLabelCol("Result")
    .setFeaturesCol("features")
    .setStepSize(0.001)

  /**
   * Fit factorization machines with training dataset
   */
  val fmModel: FMClassificationModel = fm.fit(ProcessData.assTrain)

  /**
   * Predict validation dataset with factorization machines
   */
  val validPred: DataFrame = fmModel.transform(ProcessData.assValid)

  /**
   * Compute accuracy for validation dataset
   */
  val acc: Double = Evaluator.evaluator_binary.evaluate(validPred)

  //println(acc)
}
