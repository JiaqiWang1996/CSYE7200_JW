import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ModelSpec extends AnyFlatSpec with Matchers {
  "RF Model accuracy" should "greater than 0.7" in {
    val RFacc = RF.acc
    RFacc should be > 0.7
  }

  "MLP Model accuracy" should "greater than 0.7" in {
    val MLPacc = MLP.acc
    MLPacc should be > 0.7
  }

  "LR Model accuracy" should "greater than 0.7" in {
    val LRacc = LR.acc
    LRacc should be > 0.7
  }

  "GBT Model accuracy" should "greater than 0.7" in {
    val GBTacc = GBT.acc
    GBTacc should be > 0.7
  }

  "FM Model accuracy" should "greater than 0.7" in {
    val FMacc = FM.acc
    FMacc should be > 0.7
  }

  "DT Model accuracy" should "greater than 0.6" in {
    val DTacc = DT.acc
    DTacc should be > 0.7
  }

  "SVM Model accuracy" should "greater than 0.7" in {
    val SVMacc = SVM.acc
    SVMacc should be > 0.7
  }

  "SVM coefficients number" should "equal to 9" in {
    val SVMcn = SVM.svmModel.coefficients.size
    SVMcn should equal(9)
  }

  "DT features number" should "equal to 9" in {
    val DTnf = DT.dtModel.featureImportances.size
    DTnf should equal(9)
  }

  "FM features number" should "equal to 9" in {
    val FMnf = FM.fmModel.numFeatures
    FMnf should equal(9)
  }

  "GBT features number" should "equal to 9" in {
    val GBTnf = GBT.gbtModel.featureImportances.size
    GBTnf should equal(9)
  }

  "LR coefficients number" should "equal to 9" in {
    val LRcn = LR.lrModel.coefficientMatrix.numCols
    LRcn should equal(9)
  }

  "MLP features number" should "equal to 9" in {
    val MLPwn = MLP.mlpModel.numFeatures
    MLPwn should equal(9)
  }

  "RF features number" should "equal to 9" in {
    val RFnf = RF.rfModel.featureImportances.size
    RFnf should equal(9)
  }

  "best model" should "work for accuracy > 70%" in {
    ModelSelection.testAccuracy > 0.7
  }

}
