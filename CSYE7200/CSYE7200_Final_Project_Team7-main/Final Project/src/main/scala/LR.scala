import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.DataFrame

/**
 * This object is Logistic Regression model.
 */
object LR {

  /**
   * Set parameters for logistic regression
   */
  val lr: LogisticRegression = new LogisticRegression()
    .setMaxIter(100)
    .setRegParam(0)
    .setElasticNetParam(1)
    .setLabelCol("Result")

  /**
   * Fit logistic regression with training dataset
   */
  val lrModel: LogisticRegressionModel = lr.fit(ProcessData.assTrain)

  /**
   * Predict validation dataset with logistic regression
   */
  val validPred: DataFrame = lrModel.transform(ProcessData.assValid)

  /**
   * Compute accuracy for validation dataset
   */
  val acc: Double = Evaluator.evaluator_binary.evaluate(validPred)

  //println(acc)
}
