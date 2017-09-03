
import org.apache.spark.sql.hive.HiveContext

// Create HiveContext to be able to query Hive.
val hiveContext = new HiveContext(sc)

import java.util.Date
import org.apache.commons.lang.time.DateUtils
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

import breeze.linalg.squaredDistance
import breeze.linalg.DenseVector
import scala.math.sqrt

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.SaveMode


//create DataFrame df_dimgametags, shown as the below output
//below need to be re-written to dataframe method
/*
val dimgametags : String = """
SELECT 
    *
FROM
    al.dimgametags
WHERE
    channel like '%Desktop%' 
"""
    
val df_dimgametags0 = hiveContext.sql(dimgametags)

val df_dimgametags = df_dimgametags0.filter("classification != '' and tagname != '' ")

//Retrieve provider and classification information
val df_dimgamev2_raw : String = """
SELECT 
    a.walletgameid, case b.providername when 'Nyx' then 'NyxCasino' else b.providername end as providernames, b.classification
FROM
    al.dimgametags a left join al.dimgamev2 b
ON
    a.walletgameid = b.walletgameid
WHERE
    b.channel like '%Desktop%' and b.walletgameid <> 0 and b.classification <> "" 
"""
val df_dimgamev2 = hiveContext.sql(df_dimgamev2_raw)

//remove minigames
val df_minigame_raw : String = """
SELECT 
    walletgameid
FROM
    al.dimgametags
WHERE
    channel like '%Desktop%' and tagname = "Minigame"
"""
val df_minigame = hiveContext.sql(df_minigame_raw)

//put games having these features into special group - Massive Symbols, Cascade, Colossal Reels, Reel control
val df_MS_raw : String = """
SELECT 
    walletgameid
FROM
    al.dimgametags
WHERE
    channel like '%Desktop%' and tagname = "Massive Symbols"
"""
val df_MS = hiveContext.sql(df_MS_raw)

//df_MS.show()
val Massive_symbol = df_MS.map(row => (row.getAs[Int](0))).toArray

val df_CC_raw : String = """
SELECT 
    walletgameid
FROM
    al.dimgametags
WHERE
    channel like '%Desktop%' and tagname = "Cascade"
"""
val df_CC = hiveContext.sql(df_CC_raw)

//df_CC.show()
val Cascade = df_CC.map(row => (row.getAs[Int](0))).toArray

val df_CR_raw : String = """
SELECT 
    walletgameid
FROM
    al.dimgametags
WHERE
    channel like '%Desktop%' and tagname = "Colossal Reels"
"""
val df_CR = hiveContext.sql(df_CR_raw)

//df_CR.show()
val Colossal_Reels = df_CR.map(row => (row.getAs[Int](0))).toArray

val df_RC_raw : String = """
SELECT 
    walletgameid
FROM
    al.dimgametags
WHERE
    channel like '%Desktop%' and tagname = "Reel control"
"""
val df_RC = hiveContext.sql(df_RC_raw)

//df_RC.show()
val Reel_control = df_RC.map(row => (row.getAs[Int](0))).toArray
*/

object SimilarGames {
  println("\n\nAbout to call the apply method in Similar Games companion object\n\n")
  //Is this how it's done in StreamIngester
  def apply(hc: HiveContext, dimGameTagsBroadcast: Broadcast[Option[DataFrame]], dimGamesBroadcast: Broadcast[DataFrame]): Unit =
    new SimilarGames(hc, dimGameTagsBroadcast, dimGamesBroadcast).generateList
}

class SimilarGames(hc: HiveContext, dimGameTagsBroadcast: Broadcast[Option[DataFrame]], dimGamesBroadcast: Broadcast[DataFrame])
  extends java.io.Serializable {


  if (dimGameTagsBroadcast.value.isEmpty) {
    println("al.dimgamesv2 was not retrieved successfully. Exiting SimilarGames...")
    break
  }

  val sqlContext = new org.apache.spark.sql.SQLContext(hc.sparkContext)

  import sqlContext.implicits._

  val df_dimgametagsDesktop: DataFrame = dimGameTagsBroadcast.value.get.filter($"channel" like "Desktop")

  df_dimgametagsDesktop.show(10)
  
  val df_dimgametags = df_dimgametagsDesktop.filter($"classification" !== "").filter($"tagname" !== "")

  val df_dimgamev2: DataFrame = dimGamesBroadcast.value
    //Nyx -> NyxCasino or else else leave as same
                                    .withColumn("providernames", when($"providername" === "Nyx", "NyxCasino").otherwise($"providername"))
                                    .filter($"channel" like "Desktop").filter($"walletgameid" !== 0).filter($"classification" !== "")
                                    .select("walletgameid", "providernames", "classification")

  df_dimgamev2.show()
  
  val df_minigame = dimGameTagsBroadcast.value.get.filter($"channel" like "Desktop").filter($"tagname" === "Minigame").select("walletgameid")
  
  val df_MS = dimGameTagsBroadcast.value.get.filter($"channel" like "Desktop").filter($"tagname" === "Massive Symbols").select("walletgameid")
  val df_CC = dimGameTagsBroadcast.value.get.filter($"channel" like "Desktop").filter($"tagname" === "Cascade").select("walletgameid")
  val df_CR = dimGameTagsBroadcast.value.get.filter($"channel" like "Desktop").filter($"tagname" === "Colossal Reels").select("walletgameid")
  val df_RC = dimGameTagsBroadcast.value.get.filter($"channel" like "Desktop").filter($"tagname" === "Reel control").select("walletgameid")



   def computeToMetatagsMap: RDD[(Int, Iterable[String])] = {
	    println("In computeToMetatagsMap")
	    val df_providernclass = df_dimgamev2.groupBy("walletgameid", "providernames", "classification").count().distinct()
	    
	    //The operations that we're doing on these rdds further down the line - do them in reduceByKey
	    val idToTagsMap = df_dimgametags.map(row => (row.getAs[Int](4), Array(row.getAs[String](2)))).reduceByKey(_ ++ _)

	    val idToClassificationMap = df_providernclass.map(row => (row.getAs[Int](0), Array(row.getAs[String](2)))).reduceByKey(_ ++ _)

	    val idToProviderMap = df_providernclass.map(row => (row.getAs[Int](0), Array(row.getAs[String](1)))).reduceByKey(_ ++ _)

	    //I guess we can do the flatmap before the groupby here as well?
	    //All of the xs should have a descriptive name
	    (idToTagsMap ++ idToClassificationMap ++ idToProviderMap).groupBy(_._1).map{ x => (x._1, x._2.flatMap(y => y._2)) }
	  	}

  //Hacky way to manually adjust some tags weight..

	def weightAdj(n1: Int, n2: Int, idToMetatagsMap: RDD[(Int, Iterable[String])]) :RDD [(Int, Iterable[String])] = {
	    val lower_providers = collection.mutable.ArrayBuffer("CryptoLogicCasino", "Entraction", "Evolution", "GTSPTGames", "Game360Casino", "IGTCasino", "ISoftBetCasino", "JadestoneCasino", "LeanderCasino", "MicrogamingCasino", "MicrogamingPoker", "NEOGames", "NetEntCasino", "NetEntPoker", "NyxCasino", "PlaynGoCasino", "QuickspinCasino", "RelaxGamingPoker", "Sportsbook", "ThunderKickCasino", "YggdrasilCasino")

	    val dummy1 = ArrayBuffer.fill(n1)(-999, lower_providers)  //lower down the weights for providers 

	    val lower_weights = collection.mutable.ArrayBuffer("Serious", "Happy", "Beginner", "Advanced", "Skill element")
	    val dummy2 = ArrayBuffer.fill(n2)(-999, lower_weights) 
	    
	    val hackyidToMetatagsMap = dummy1 ++ dummy2 ++ idToMetatagsMap.toArray.toBuffer
	    sc.parallelize(hackyidToMetatagsMap)
		}


  def calculateTfidf(idToMetatagsMap: RDD[(Int, Iterable[String])]): Array[(Int, Vector)] = {
    println("In calculateTfidf")
    //Why do we need to turn this into an array?
    //A lot of these things here we only need to do once.
    //Can I extract methods from this content?
    val tagsArray = idToMetatagsMap.flatMap(x => x._2).distinct.collect()

    val df_Metatags = idToMetatagsMap.map{ x => (x._1, x._2 mkString " ") }.toDF("id", "tags")

    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    val wordsData = tokenizer.transform(df_Metatags)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")//.setNumFeatures(tagsArray.size + 1) // set number of feature equals to the number of metatags
    
    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    //What's this line doing?
//    rescaledData.select("id", "features")

    rescaledData.select("id", "features").rdd.map { row => (row.getAs[Int](0), row.getAs[Vector](1))}.collect()
  }

  implicit def bool2int(a:Boolean) = if (a) 1 else 0

	def containMS(id:Int, Massive_symbol:Array[Int] ):  Double = {
	    Massive_symbol.toArray.contains(id).toDouble
	}
	containMS(105, Massive_symbol)
	def containCC(id:Int, Cascade:Array[Int]):  Double = {
	    Cascade.toArray.contains(id).toDouble
	}
	def containCR(id:Int, Colossal_Reels:Array[Int] ):  Double = {
	    Colossal_Reels.toArray.contains(id).toDouble
	}
	def containRC(id:Int, Reel_control:Array[Int] ):  Double = {
	    Reel_control.toArray.contains(id).toDouble
	}


def computeTfidfDistance(id1: Int, id2: Int, tfidf:  Array[(Int, Vector)]) : Double = {
    println("In computeTfidfDistance")
//    val tagVector1 = new DenseVector(tfidf.lookup(id1).head.toArray)
    val tagVector1 = new DenseVector(tfidf.filter(_._1 == id1).head._2.toArray)//.head)//._2.toArray)//lookup(id1).head.toArray)
//    val x: String = tagVector1
//    val y: String = tfidf.filter(_._1 == id1).map(_._2)//.head//.head
//    println("tagVector1 size is: " + tagVector1.activeSize)
    val tagVector2 = new DenseVector(tfidf.filter(_._1 == id2).head._2.toArray)
    sqrt(squaredDistance(tagVector1, tagVector2))
  }



def distance_adj(id1: Int, id2: Int) : Double = {
    val distance_adj: Double = containMS(id1, Massive_symbol) * containMS(id2, Massive_symbol) * (-3.0) + containCC(id1, Cascade) * containCC(id2, Cascade) * (-3.0) + containCR(id1, Colossal_Reels) * containCR(id2, Colossal_Reels) * (-3.0) + containRC(id1, Reel_control) * containRC(id2, Reel_control) * (-3.0) 
    distance_adj
	}


  def otherWalletGameIds(walletGameId: Int, walletgameids: DataFrame) = {
    println("In otherWalletGameIds")
    //Can we avoid collecting here?
    walletgameids.filter( walletgameids("walletgameid") !== walletGameId ).map(row => row.getAs[Int](0))//.collect()  //TAKE THE FIRST 10 GAMES FOR TESTING
  }

  //(id1, id2, rank, distance)
  def kNNTfidf(id: Int, k: Int, tfidf:  Array[(Int, Vector)], walletgameids: DataFrame): List[(Int, Int, Double)] = {
    println("In kNNTfidf\n")
    //Cache this
    val otherGameId = otherWalletGameIds(id, walletgameids)
    println("about to cache otherGameId...\n")
    otherGameId.cache()
    println("cached otherGameId\n")
    //Why can't this be an RDD? Because they're immutable
    //Broadcast this. No because the changes on the box don't get preserved.
//    var distanceCollection = ListBuffer[(Int, Int, Double)]()

    //We should be able to iterate over othergameid. We must do so in order to make this distributable?
//    for (i: Int <- 1 to otherGameId.size){
    //Doesn't seem to be any advantage to doing it this way.
    //I need to do some kind of reduce operation. otherGameId as Rdd, make tuple with unique id, and then map over creating the distance collection!
    val distanceCollection = otherGameId.map(x => (id, x, computeTfidfDistance(id, x, tfidf) + distance_adj(id, x)))
    println("Computed distanceCollection apparently\n")
//    (1 to otherGameId.size).foreach { x =>
//      val distance: Double = computeTfidfDistance(id,otherGameId(i-1), tfidf:  RDD[(Int, Vector)])
//      val distance: Double = computeTfidfDistance(id,otherGameId(x-1), tfidf:  RDD[(Int, Vector)])
//      distanceCollection = distanceCollection:+ ((id,otherGameId(i-1), distance ))
//      distanceCollection = distanceCollection:+ ((id,otherGameId(x-1), distance ))
//      println("size of buffer is: " + distanceCollection.size)
//    }

    println("About to sort distanceCollection\n")
    val x = distanceCollection.collect().toList.sortBy(x => (x._3, -x._2)).take(k)    //sort by distance and walletid
    println("size of object being returned by kNNTfidf is: " + x.size)
    x
  }
//Iterate through all games
def returnResult(topGame: Array[Int], tfidf:  Array[(Int, Vector)],  walletgameids: DataFrame ): List[(Int, Int, Double)] = {
    
    val result: List[(Int, Int, Double)]  = topGame.map(id => (kNNTfidf(id, 20, tfidf.toArray, walletgameids))).flatMap(a => a).toList
    
    result
    
}

def writeResult(result: DataFrame): Unit = {
    //Should I partition by a column of a table?
    Try(result.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("datascience.similar_game_testing")) match {
      case Success(x) => println(s"\n\nSuccessfully wrote to datascience.similar_game_testing in Hive: ${x.toString}\n\n")
      case Failure(ex) => println(s"\n\nFailed to write similar_game_testing to Hive: ${ex.getMessage}\n\n")
    }
  }

  def generateList: Unit = {
    println("\n\nIn generateList in SimilarGames\n\n")

    val idToMetatagsMap: RDD[(Int, Iterable[String])] = computeToMetatagsMap

    val hackyidToMetatagsMap = weightAdj(0, 0, idToMetatagsMap)
    
    //Cache it? How big is it?
    val tfidf: Array[(Int, Vector)] = calculateTfidf(hackyidToMetatagsMap)
    //hc.sparkContext.broadcast(tfidf)

    //calculate pair distance
    computeTfidfDistance(147, 763, tfidf)

    val walletgameids0 = df_dimgametags.select(df_dimgametags("walletgameid")).distinct()
    val walletgameidsArray0 = walletgameids0.map(row => (row.getAs[Int](0))).toArray
    val minigame = df_minigame.map(row => (row.getAs[Int](0))).toArray
    val walletgameidsArray = walletgameidsArray0 diff minigame
    val walletgameids: DataFrame = sc.parallelize(walletgameidsArray).toDF("walletgameid")

    otherWalletGameIds(147, walletgameids)

    kNNTfidf(790, 10, tfidf, walletgameids)
    
    val topGame = Array(790, 225, 1405, 22, 69, 1535, 1575, 246, 112, 1842, 
                 2252, 542, 778, 1827/*, 1332*/, 754, 275, 2135, 1796, 2051) //top 20 slots

    val Result0 = returnResult(topGame, tfidf, walletgameids )
    val Result = sc.parallelize(Result0).toDF("game1", "game2", "distance")
    val resultFinal = Result.select($"game1", $"game2", $"distance", row_number().over(Window.partitionBy($"game1")).as("rn"))
    writeResult(resultFinal)
    
    
    println("Reached the end of generateList..\n")
    
    val sampleGame = Array(790, 1180, 1205, 1262, 1370, 1446,225) //starburst
    
    for (i: Int <- 1 to sampleGame.size){
    println(sc.parallelize(tfidf).lookup(sampleGame(i-1)))
   
}
  }
  
  generateList  

}