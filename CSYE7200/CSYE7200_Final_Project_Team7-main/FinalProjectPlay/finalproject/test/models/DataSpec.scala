package models

import models.ProcessData._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataSpec extends AnyFlatSpec with Matchers {

  behavior of "raw dataset"
  it should "work for row length" in {
    val rowLenBans = bans.count()
    val rowLenGold = gold.count()
    val rowLenKills = kills.count()
    val rowLenMatch = matchinfo.count()
    val rowLenMons = monsters.count()
    val rowLenStrucs = structures.count()
    rowLenBans shouldBe 15240
    rowLenGold shouldBe 99060
    rowLenKills shouldBe 191069
    rowLenMatch shouldBe 7620
    rowLenMons shouldBe 44248
    rowLenStrucs shouldBe 121386
  }

  it should "work for column length" in {
    val colLenBans = bans.columns.length
    val colLenGold = gold.columns.length
    val colLenKills = kills.columns.length
    val colLenMatch = matchinfo.columns.length
    val colLenMons = monsters.columns.length
    val colLenStrucs = structures.columns.length
    colLenBans shouldBe 7
    colLenGold shouldBe 97
    colLenKills shouldBe 11
    colLenMatch shouldBe 30
    colLenMons shouldBe 4
    colLenStrucs shouldBe 5
  }

  it should "work for value restrictions" in {
    val numRedTeam = bans.filter(bans("Team") === "redBans").count()
    val numBlueTeam = bans.filter(bans("Team") === "blueBans").count()
    numRedTeam shouldBe numBlueTeam

    val killsTime = kills.filter(kills("Time") <= 0).count()
    killsTime shouldBe 0

    val numRedWin = matchinfo.filter(matchinfo("rResult") === 1).count()
    val numBlueLose = matchinfo.filter(matchinfo("bResult") === 0).count()
    numRedWin shouldBe numBlueLose

    val numRedLose = matchinfo.filter(matchinfo("rResult") === 0).count()
    val numBlueWin = matchinfo.filter(matchinfo("bResult") === 1).count()
    numRedLose shouldBe numBlueWin

    val redRes = matchinfo.filter(!matchinfo("rResult").isin(0, 1)).count()
    val blueRes = matchinfo.filter(!matchinfo("bResult").isin(0, 1)).count()
    redRes shouldBe 0
    blueRes shouldBe 0

    val monsTime = monsters.filter(monsters("Time") <= 0).count()
    monsTime shouldBe 0

    val monsType = monsters.filter(!monsters("type").isin("DRAGON", "AIR_DRAGON", "BARON_NASHOR", "EARTH_DRAGON", "ELDER_DRAGON", "FIRE_DRAGON", "FIRE_DRAGON", "WATER_DRAGON", "RIFT_HERALD")).count()
    monsType shouldBe 0

    val strucTime = structures.filter(structures("Time") <= 0).count()
    strucTime shouldBe 0
  }

  behavior of "processed data"
  it should "work for gold equation" in {
    val goldEq = goldDiff.filter(!(goldDiff("team_15") === (goldDiff("top_15") + goldDiff("mid_15") + goldDiff("jun_15") + goldDiff("sup_15") + goldDiff("adc_15")))).count()
    goldEq shouldBe 0
  }

  it should "work for no-null values" in {
    val nullDrags = predData.filter(predData("Dragons").isNull).count()
    nullDrags shouldBe 0

    val nullStrucs = predData.filter(predData("Structures").isNull).count()
    nullStrucs shouldBe 0

    val nullKills = predData.filter(predData("Kills").isNull).count()
    nullKills shouldBe 0
  }

  it should "work for proportion" in {
    val numData = predData.count()
    numData shouldBe 15240

    val numTest = test.count().toDouble
    val numTrain = train.count().toDouble
    val numValid = valid.count().toDouble

    numTest / 15240 shouldBe 0.1 +- 0.05
    numValid / (numTrain + numValid) shouldBe 0.3 +- 0.05
  }
}
