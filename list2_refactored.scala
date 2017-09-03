package game.recommendations.lists

import breeze.linalg.{DenseVector, squaredDistance}
import game.recommendations.Settings
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.joda.time.LocalDate

import scala.collection.mutable.Buffer
import scala.math.sqrt
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}


/**
  * Created by cogi01 on 30/01/2017.
  */

class SimilarGames(dimGamesBroadcast: Broadcast[DataFrame], dimGameTagsBroadcast: Broadcast[Option[DataFrame]], tableMappingsOption: Option[DataFrame], listLog: Integer)
                  (implicit hc: HiveContext, @transient conf: Settings) extends RecommendationGenerator with java.io.Serializable {

  import hc.implicits._

  val log = listLog

  val distanceAdjustment = 3.0

  val numOfResults = 50

  val resultsSchema = new StructType(Array(StructField("game1",IntegerType,nullable = true),StructField("game2",IntegerType,nullable = true),StructField("distance",DoubleType,nullable = true)))

  val gametags: DataFrame = dimGameTagsBroadcast.value.get.filter($"classification" !== "").filter($"tagname" !== "")

  val games: DataFrame = dimGamesBroadcast.value
      .withColumn("providernames", when($"providername" === "Nyx", "NyxCasino").otherwise($"providername"))
      .filter($"walletgameid" !== 0).filter($"classification" !== "")
      .select("walletgameid", "providernames", "classification", "channel")

  val tableMappings = tableMappingsOption.get

  val minigames: DataFrame = gametags.filter($"tagname" === "Minigame").select("walletgameid", "channel")

  val massiveSymbolsArray: Array[(Int, String)] = toTupleArray(gametags.filter($"tagname" === "Massive Symbols").select("walletgameid", "channel"))
  val cascadesArray: Array[(Int, String)] = toTupleArray(gametags.filter($"tagname" === "Cascade").select("walletgameid", "channel"))
  val colossolReelsArray: Array[(Int, String)] = toTupleArray(gametags.filter($"tagname" === "Colossal Reels").select("walletgameid", "channel"))
  val reelControlArray: Array[(Int, String)] = toTupleArray(gametags.filter($"tagname" === "Reel control").select("walletgameid", "channel"))

  val gameArrays = Set(massiveSymbolsArray, cascadesArray, colossolReelsArray, reelControlArray)

  def generateRecommendations(): Unit = {

    println("in list 2 generateRecommendations")

    val today = new LocalDate()

    Try(assert(dimGameTagsBroadcast.value.nonEmpty && tableMappingsOption.nonEmpty,
          "At least one of al.dimgametags and table_mappings_gre_enabled was not retrieved successfully. Exiting SimilarGames...")) match {
      case Success(_) => Unit
      case Failure(e) => println(exDetails(e)); endLoggingError(log, e); break()
    }

    val channels = Set("Desktop", "Mobile")

    truncateRecommendations(log)

    channels.foreach{channel =>

      breakable{

        println("In foreach for " + channel)

        val channelLog = startLogging(s"GRE v2 Similar Games $channel")

        val tfidf: Array[(Int, Vector)] = calculateTfidf(computeToMetatagsMap(channel).collect().toBuffer)

        val walletGameIds: DataFrame = gametags.filter($"channel" like s"%$channel%").filter($"walletgameid" !== -1).select(gametags("walletgameid")).distinct()

        val minigamesArray: Array[Int] = minigames.filter($"channel" like s"$channel").map(row => (row.getAs[Int](0))).collect()
        val walletGameIdsNoMiniGames: Array[Int] = walletGameIds.map(row => (row.getAs[Int](0))).collect() diff minigamesArray

        val walletGameIdsNoMiniGamesDf = hc.sparkContext.parallelize(walletGameIdsNoMiniGames).toDF("walletgameid")

        val allGames: Array[Int] = walletGameIdsNoMiniGamesDf.select("walletgameid").rdd.map(r => r.getAs[Int](0)).collect()

        val results: Seq[(Int, Int, Double)] = returnResult(allGames, tfidf, walletGameIdsNoMiniGamesDf, channel, channelLog)

        println("about to compute resultsDF")
        val resultsDf = hc.createDataFrame(hc.sparkContext.parallelize(results.map(x => Row(x._1, x._2, x._3))), resultsSchema)

        println("about to compute baseRecommendations")

        val baseRecommendations = resultsDf
          .join(dimGamesBroadcast.value.select($"walletgameid", $"gameid", $"providername"), $"game1" === $"walletgameid", "leftouter")
          .select($"gameid".alias("g1"), $"game2", lit(channel).as("devicetype"), $"distance", $"providername")
          .join(dimGamesBroadcast.value.select($"walletgameid", $"gameid"), $"game2" === $"walletgameid", "leftouter").repartition(3000).cache()

        println("Successfully computed baseRecommendations")

        tableMappings.collect().foreach { customerGroup =>

          println("In table mappings for each")

          val brandsGroup = customerGroup.getString(0)
          println("\n\nbrandsGroup is: " + brandsGroup + "\n\n")

          val brandId = customerGroup.getInt(1)
          println("\n\nbrandId is: " + brandId + "\n\n")

          val countryCode = customerGroup.getString(2)
          println("countryCode is: " + countryCode +"\n\n")

          val recommendations = baseRecommendations
                .select($"g1", $"gameid".alias("g2"), $"providername", row_number().over(Window.partitionBy($"g1").orderBy($"g1", $"distance")).as("rn"),
                  lit(brandsGroup).as("tablename"), lit(brandId).as("brandid"), lit(countryCode).as("countrycode"), $"devicetype", lit(today.toString).as("rundate"))

          writeRecommendations(recommendations, channel)

        }

        endLogging(channelLog)
      }
    }
    endLogging(log)
  }

  def toTupleArray(dataFrame: DataFrame): Array[(Int, String)] = {
    dataFrame.map(row => (row.getAs[Int](0), row.getAs[String](1))).collect()
  }

  val boolToBinary: (Boolean => Int) = (a: Boolean) => if (a) 1 else 0

  def computeToMetatagsMap(channel: String): RDD[(Int, Iterable[String])] = {

    val providerWithClass: DataFrame = games.filter($"channel" like s"%$channel%").groupBy("walletgameid", "providernames", "classification").count().distinct().repartition(3000)

    val idToTagsMap: RDD[(Int, Array[String])] = gametags.filter($"channel" like s"%$channel%").map(row => (row.getAs[Int](4), Array(row.getAs[String](2)))).reduceByKey(_ ++ _)

    val idToClassificationMap: RDD[(Int, Array[String])] = providerWithClass.map(row => (row.getAs[Int](0), Array(row.getAs[String](2)))).reduceByKey(_ ++ _)

    val idToProviderMap: RDD[(Int, Array[String])] = providerWithClass.map(row => (row.getAs[Int](0), Array(row.getAs[String](1)))).reduceByKey(_ ++ _)

    (idToTagsMap ++ idToClassificationMap ++ idToProviderMap).groupBy(_._1).map{ x => (x._1, x._2.flatMap(tags => tags._2)) }.coalesce(300)
  }

  def calculateTfidf(idToMetatagsMap: Buffer[(Int, Iterable[String])]): Array[(Int, Vector)] = {

    val metaTags = idToMetatagsMap.map{ x => (x._1, x._2 mkString " ") }.toDF("id", "tags")

    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    val wordsData: DataFrame = tokenizer.transform(metaTags)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")//.setNumFeatures(tagsArray.size + 1) // set number of feature equals to the number of metatags

    val featurizedData: DataFrame = hashingTF.transform(wordsData)

    val idfModel = new IDF().setInputCol("rawFeatures").setOutputCol("features").fit(featurizedData)

    val rescaledData: DataFrame = idfModel.transform(featurizedData)

    rescaledData.select("id", "features").map { row => (row.getAs[Int](0), row.getAs[Vector](1))}.collect()
  }

  def computeTfidfDistance(id1: Int, id2: Int, tfidf: Array[(Int, Vector)]) : Double = {

    val tagVectorId1 = new DenseVector(tfidf.filter(_._1 == id1).head._2.toArray)

    val tagVectorId2 = new DenseVector(tfidf.filter(_._1 == id2).head._2.toArray)
    sqrt(squaredDistance(tagVectorId1, tagVectorId2))
  }

  def distanceAdj(id1: Int, id2: Int, adj: Double, channel: String) : Double = {

    def containsId(id: Int, gameArray: Array[Int]): Double = {
      boolToBinary(gameArray.contains(id))
    }

    def gameToGameid(array: Array[(Int, String)]): Array[Int] = {
      array.filter(_._2.contains(channel)).map(_._1)
    }

    def distanceAdjForGame(gameArray: Array[(Int, String)]): Double = {
      containsId(id1, gameToGameid(gameArray)) * containsId(id2, gameToGameid(gameArray)) * (-adj)
    }

    gameArrays.map(distanceAdjForGame(_)).sum
  }

  def otherWalletGameIds(walletGameId: Int, walletgameids: DataFrame) = {

    walletgameids.filter(walletgameids("walletgameid") !== walletGameId ).map(row => row.getAs[Int](0))
  }

  def kNNTfidf(id: Int, numOfResults: Int, tfidf:  Array[(Int, Vector)], walletgameids: DataFrame, channel: String): scala.collection.immutable.Vector[(Int, Int, Double)] = {

    val distanceCollection = otherWalletGameIds(id, walletgameids).map(x => (id, x, computeTfidfDistance(id, x, tfidf) + distanceAdj(id, x, distanceAdjustment, channel))).coalesce(180)

    distanceCollection.collect().toVector.sortBy(x => (x._3, -x._2)).take(numOfResults)
  }

  def returnResult(topGame: Array[Int], tfidf:  Array[(Int, Vector)],  walletgameids: DataFrame, channel: String, log: Integer):
          scala.collection.immutable.Vector[(Int, Int, Double)] = {
    Try(topGame.map(id => (kNNTfidf(id, numOfResults, tfidf, walletgameids, channel))).flatten.toVector ensuring(_.nonEmpty, "List 2 results are empty")) match {
      case Success(results) => results
      case Failure(ex) => println(exDetails(ex)); endLoggingError(log, ex); break()
    }
  }

  def truncateRecommendations(log: Integer): Unit = {
    Try(hc.sql(s"truncate table ${conf.list2Table}")) match {
      case Success(x) => println(s"\n\nSuccessfully truncated ${conf.list2Table} in Hive: ${x.toString}\n\n")
      case Failure(ex) => println(exDetails(ex)); endLoggingError(log, ex); break()
    }
  }

  def writeRecommendations(result: DataFrame, channel: String): Unit = {
    Try(result.write.format("orc").mode(SaveMode.Append).saveAsTable(conf.list2Table)) match {
      case Success(x) => println(s"\n\nSuccessfully wrote to ${conf.list2Table} in Hive: ${x.toString}\n\n")
      case Failure(ex) => println(s"\n\nFailed to write list2 to ${conf.list2Table} in Hive: ${exDetails(ex)}\n\n")
    }
  }

}