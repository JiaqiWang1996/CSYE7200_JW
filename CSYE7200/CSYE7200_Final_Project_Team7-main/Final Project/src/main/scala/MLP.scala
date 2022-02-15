import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.sql.DataFrame

/**
 * This object is Multilayer Perceptron model.
 */
object MLP {

  /**
   * Set parameters for multilayer perceptron
   */
  val mlp: MultilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
    .setLabelCol("Result")
    .setFeaturesCol("features")
    .setLayers(Array(9, 8, 2))
    .setBlockSize(128)
    .setSeed(4444L)
    .setMaxIter(100)

  /**
   * Fit decision tree with multilayer perceptron
   */
  val mlpModel: MultilayerPerceptronClassificationModel = mlp.fit(ProcessData.assTrain)

  /**
   * Predict validation dataset with multilayer perceptron
   */
  val validPred: DataFrame = mlpModel.transform(ProcessData.assValid)

  /**
   * Compute accuracy for validation dataset
   */
  val acc: Double = Evaluator.evaluator_binary.evaluate(validPred)

  //println(acc)
}
