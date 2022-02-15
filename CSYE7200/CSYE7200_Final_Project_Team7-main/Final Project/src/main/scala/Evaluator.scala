import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

/**
 * This object is function to evaluate accuracy
 */
object Evaluator {

  /**
   * Set parameters for binaryClassificationEvaluator
   */
  val evaluator_binary: BinaryClassificationEvaluator = new BinaryClassificationEvaluator()
    .setLabelCol("Result")
    .setRawPredictionCol("rawPrediction")
    .setMetricName("areaUnderROC")
}
