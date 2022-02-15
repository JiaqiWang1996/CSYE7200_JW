package models

import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This object is to process data
 */
object ProcessData {
  /**
   * Initialize SparkSession
   */
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ProcessData")
    .master("local[*]")
    .getOrCreate()

  /**
   * Read all needed dataset from resources
   */
  val bans: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("app/resources/bans.csv")

  //bans.printSchema()
  //bans.show()

  val gold: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("app/resources/gold.csv")

  //gold.printSchema()
  //gold.show()

  val kills: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("app/resources/kills.csv")

  //kills.printSchema()
  //kills.show()

  val matchinfo: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("app/resources/matchinfo.csv")

  //matchinfo.printSchema()
  //matchinfo.show()

  val monsters: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("app/resources/monsters.csv")

  //monsters.printSchema()
  //monsters.show()

  val structures: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("app/resources/structures.csv")

  //structures.printSchema()
  //structures.show()

  /**
   * Process dataset "gold"
   */
  gold.createOrReplaceTempView("gold")

  /*
  Team gold difference
   */
  val teamDiff: DataFrame = spark.sql("SELECT Address, min_15 AS teamGoldDiff_15 FROM gold WHERE Type = 'golddiff'")
  teamDiff.createOrReplaceTempView("teamDiff")

  /*
  Top gold difference
   */
  val topBlue: DataFrame = spark.sql("SELECT Address, min_15 AS topBlueGold_15 FROM gold WHERE Type = 'goldblueTop'")
  topBlue.createOrReplaceTempView("topBlue")

  val topRed: DataFrame = spark.sql("SELECT Address, min_15 AS topRedGold_15 FROM gold WHERE Type = 'goldredTop'")
  topRed.createOrReplaceTempView("topRed")

  val topDiff: DataFrame = spark.sql("SELECT b.Address, b.topBlueGold_15 - r.topRedGold_15 AS topGoldDiff_15 FROM topBlue b JOIN topRed r ON b.Address = r.Address")
  topDiff.createOrReplaceTempView("topDiff")

  /*
  Jungle gold difference
   */
  val junBlue: DataFrame = spark.sql("SELECT Address, min_15 AS jungleBlueGold_15 FROM gold WHERE Type = 'goldblueJungle'")
  junBlue.createOrReplaceTempView("junBlue")

  val junRed: DataFrame = spark.sql("SELECT Address, min_15 AS jungleRedGold_15 FROM gold WHERE Type = 'goldredJungle'")
  junRed.createOrReplaceTempView("junRed")

  val junDiff: DataFrame = spark.sql("SELECT b.Address, b.jungleBlueGold_15 - r.jungleRedGold_15 AS jungleGoldDiff_15 FROM junBlue b JOIN junRed r ON b.Address = r.Address")
  junDiff.createOrReplaceTempView("junDiff")

  /*
  Middle gold difference
   */
  val midBlue: DataFrame = spark.sql("SELECT Address, min_15 AS middleBlueGold_15 FROM gold WHERE Type = 'goldblueMiddle'")
  midBlue.createOrReplaceTempView("midBlue")

  val midRed: DataFrame = spark.sql("SELECT Address, min_15 AS middleRedGold_15 FROM gold WHERE Type = 'goldredMiddle'")
  midRed.createOrReplaceTempView("midRed")

  val midDiff: DataFrame = spark.sql("SELECT b.Address, b.middleBlueGold_15 - r.middleRedGold_15 AS middleGoldDiff_15 FROM midBlue b JOIN midRed r ON b.Address = r.Address")
  midDiff.createOrReplaceTempView("midDiff")

  /*
  ADC gold difference
   */
  val adcBlue: DataFrame = spark.sql("SELECT Address, min_15 AS adcBlueGold_15 FROM gold WHERE Type = 'goldblueADC'")
  adcBlue.createOrReplaceTempView("adcBlue")

  val adcRed: DataFrame = spark.sql("SELECT Address, min_15 AS adcRedGold_15 FROM gold WHERE Type = 'goldredADC'")
  adcRed.createOrReplaceTempView("adcRed")

  val adcDiff: DataFrame = spark.sql("SELECT b.Address, b.adcBlueGold_15 - r.adcRedGold_15 AS adcGoldDiff_15 FROM adcBlue b JOIN adcRed r ON b.Address = r.Address")
  adcDiff.createOrReplaceTempView("adcDiff")

  /*
  Support gold difference
   */
  val supBlue: DataFrame = spark.sql("SELECT Address, min_15 AS supportBlueGold_15 FROM gold WHERE Type = 'goldblueSupport'")
  supBlue.createOrReplaceTempView("supBlue")

  val supRed: DataFrame = spark.sql("SELECT Address, min_15 AS supportRedGold_15 FROM gold WHERE Type = 'goldredSupport'")
  supRed.createOrReplaceTempView("supRed")

  val supDiff: DataFrame = spark.sql("SELECT b.Address, b.supportBlueGold_15 - r.supportRedGold_15 AS supportGoldDiff_15 FROM supBlue b JOIN supRed r ON b.Address = r.Address")
  supDiff.createOrReplaceTempView("supDiff")

  /*
  Combine dataframe
   */
  val goldDiff: DataFrame = spark.sql("SELECT team.Address AS Address, team.teamGoldDiff_15 AS team_15, t.topGoldDiff_15 AS top_15, j.jungleGoldDiff_15 AS jun_15, m.middleGoldDiff_15 AS mid_15, a.adcGoldDiff_15 AS adc_15, s.supportGoldDiff_15 AS sup_15 " +
    "FROM teamDiff team JOIN topDiff t ON team.Address = t.Address" +
    " JOIN midDiff m ON team.Address = m.Address" +
    " JOIN junDiff j ON team.Address = j.Address" +
    " JOIN adcDiff a ON team.Address = a.Address" +
    " JOIN supDiff s ON team.Address = s.Address")

  goldDiff.createOrReplaceTempView("goldDiff")

  val redGoldDiff: DataFrame = spark.sql("SELECT Address, team_15 * (-1) AS team_15, top_15 * (-1) AS top_15, jun_15 * (-1) AS jun_15, mid_15 * (-1) AS mid_15, adc_15 * (-1) AS adc_15, sup_15 * (-1) AS sup_15 FROM goldDiff")
  redGoldDiff.createOrReplaceTempView("redGoldDiff")

  //goldDiff.show()
  //redGoldDiff.show()

  /**
   * Process dataset "mosters"
   */
  monsters.createOrReplaceTempView("monsters")

  /*
  Blue team's monster info
   */
  val blueDragon: DataFrame = spark.sql("select Address, Count(Type) AS BlueDragon " +
    "from monsters where Time <= 15 AND Team in ('bDragons','bHeralds')" +
    "group by Address")

  blueDragon.createOrReplaceTempView("blueDragon")

  //blueDragon.show()

  /*
  Red team's monster info
   */
  val redDragon: DataFrame = spark.sql("select Address,  Count(Type) AS RedDragon " +
    "from monsters where Time <= 15 AND Team in ('rDragons','rHeralds')" +
    "group by Address")

  redDragon.createOrReplaceTempView("redDragon")

  //redDragon.show()


  /**
   * Process dataset "structures"
   */
  structures.createOrReplaceTempView("structures")

  /*
  Structures info
   */
  val blueStruc: DataFrame = spark.sql("SELECT Address, COUNT(Type) AS BlueStruc FROM structures WHERE Time <= 15 AND Team IN ('bTowers', 'bInhibs') GROUP BY Address")
  val redStruc: DataFrame = spark.sql("SELECT Address, COUNT(Type) AS RedStruc FROM structures WHERE Time <= 15 AND Team IN ('rTowers', 'rInhibs') GROUP BY Address")
  blueStruc.createOrReplaceTempView("blueStruc")
  redStruc.createOrReplaceTempView("redStruc")

  //blueStruc.show()
  //redStruc.show()


  /**
   * Process dataset "matchinfo"
   */
  matchinfo.createOrReplaceTempView("matchinfo")

  val matchResult: DataFrame = spark.sql("SELECT Address, bResult, rResult FROM matchinfo")
  matchResult.createOrReplaceTempView("matchResult")

  //matchResult.show()


  /**
   * Process dataset "kills"
   */
  kills.createOrReplaceTempView("kills")

  /*
  Kills info
   */
  val blueKills: DataFrame = spark.sql("SELECT Address, COUNT(Killer) AS BlueKills FROM kills WHERE Time <= 15 AND Killer != 'TooEarly' AND Team = 'bKills' GROUP BY Address")
  val redKills: DataFrame = spark.sql("SELECT Address, COUNT(Killer) AS RedKills FROM kills WHERE Time <= 15 AND Killer != 'TooEarly' AND Team = 'rKills' GROUP BY Address")
  blueKills.createOrReplaceTempView("blueKills")
  redKills.createOrReplaceTempView("redKills")

  //RedKills.show()
  //Bluekills.show()

  /**
   * Create dataset needed to used for training
   */
  val predData: DataFrame = spark.sql("SELECT g.team_15 AS TeamDiff, g.top_15 AS TopDiff, g.jun_15 AS JunDiff, g.mid_15 AS MidDiff, g.adc_15 AS ADCDiff, g.sup_15 AS SupDiff, " +
    " CAST(CASE WHEN d.BlueDragon IS NULL THEN 0 ELSE d.BlueDragon END AS integer) AS Dragons," +
    " CAST(CASE WHEN s.BlueStruc IS NULL THEN 0 ELSE s.BlueStruc END AS integer) AS Structures, " +
    " CAST(CASE WHEN k.BlueKills IS NULL THEN 0 ELSE k.BlueKills END AS integer) AS Kills, r.bResult AS Result" +
    " FROM goldDiff g LEFT JOIN blueDragon d ON g.Address = d.Address" +
    " LEFT JOIN blueStruc s ON g.Address = s.Address" +
    " LEFT JOIN blueKills k ON g.Address = k.Address" +
    " JOIN matchResult r ON g.Address = r.Address" +
    " UNION" +
    " SELECT g.team_15 AS TeamDiff, g.top_15 AS TopDiff, g.jun_15 AS JunDiff, g.mid_15 AS MidDiff, g.adc_15 AS ADCDiff, g.sup_15 AS SupDiff, " +
    " CAST(CASE WHEN d.RedDragon IS NULL THEN 0 ELSE d.RedDragon END AS integer) AS Dragons," +
    " CAST(CASE WHEN s.RedStruc IS NULL THEN 0 ELSE s.RedStruc END AS integer) AS Structures, " +
    " CAST(CASE WHEN k.RedKills IS NULL THEN 0 ELSE k.RedKills END AS integer) AS Kills, r.rResult AS Result" +
    " FROM redGoldDiff g LEFT JOIN redDragon d ON g.Address = d.Address" +
    " LEFT JOIN redStruc s ON g.Address = s.Address" +
    " LEFT JOIN redKills k ON g.Address = k.Address" +
    " JOIN matchResult r ON g.Address = r.Address")

  //print(predData.count())
  //predData.printSchema()

  /**
   * Split dataset into training, validation and testing
   */
  val Array(train_valid, test) = predData.randomSplit(Array(0.9, 0.1), seed = 11111L)
  val Array(train, valid) = train_valid.randomSplit(Array(0.7, 0.3), seed = 22222L)

  //print(test.count())
  //print(train.count())
  //print(valid.count())
  //spark.close()

  /**
   * Select features from dataset
   */
  val assembler: VectorAssembler = new VectorAssembler()
    .setInputCols(Array("TeamDiff", "TopDiff", "JunDiff", "MidDiff", "ADCDiff", "SupDiff", "Dragons", "Structures", "Kills"))
    .setOutputCol("features")

  val assTrain: DataFrame = assembler.transform(train)

  val assValid: DataFrame = assembler.transform(valid)

  val assTest: DataFrame = assembler.transform(test)

  /**
   * Standardize features
   */
  val scaler: StandardScaler = new StandardScaler()
    .setInputCol(assembler.getOutputCol)
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(true)

  val scalerModel: StandardScalerModel = scaler.fit(assTrain)

  val scaleAssTrain: DataFrame = scalerModel.transform(assTrain)

  val scaleAssValid: DataFrame = scalerModel.transform(assValid)

  val scaleAssTest: DataFrame = scalerModel.transform(assTest)
}
