import org.apache.spark.ml.linalg._
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by karen on 17-9-27.
  */

case class Sample(id:String,
                  review:String,
                  sentiment:Option[Int]=None)

object word2Vec extends App{



  val conf=new SparkConf()
    .setMaster("local")
    .setAppName("word2vec")

  val sc=new SparkContext(conf)

  val train_path=s"labeledTrainData.tsv"
  val test_path=s"testData.tsv"

  def skipHeaders(index:Int, iter:Iterator[String])=if (index==0) iter.drop(1) else iter

  val train_file=sc.textFile(train_path) mapPartitionsWithIndex skipHeaders map(_.split("\t"))
  val test_file=sc.textFile(test_path) mapPartitionsWithIndex skipHeaders map(_.split("\t"))

  def toSample(segments:Array[String])=segments match {
      case Array(id, sentiment, review) =>Sample(id, review, Some(sentiment.toInt))
      case Array(id, review) =>Sample(id, review)
  }

  val train_samples=train_file map toSample
  val test_samples=test_file map toSample


  def toBreeze(vector: Vector) : breeze.linalg.Vector[scala.Double] = vector match {
    case sv: SparseVector => new breeze.linalg.SparseVector[Double](sv.indices, sv.values, sv.size)
    case dv: DenseVector => new breeze.linalg.DenseVector[Double](dv.values)
  }

  def cleanHtml(html_str:String):String=html_str.replaceAll("""<(?!\/?a(?=>\s.*>))\/?.*?>""","")

  def cleanSampleHtml(sample_html:Sample)=sample_html copy (review=cleanHtml(sample_html.review))

  val cleanTrainSamples=train_samples map cleanSampleHtml

  val cleanTestSamples=test_samples map cleanSampleHtml

  // extract words
  def cleanWord(str:String)=
    str
      .split(" ")
      .map(_.trim.toLowerCase)
      .filter(_.size>0)
      .map(_.replaceAll("\\W+", ""))
      .reduce{(x,y)=>s"$x, $y"}

  def wordOnlySample(sample: Sample)=sample copy (review=cleanWord(sample.review))

  val wordOnlyTrainSample=cleanTrainSamples map wordOnlySample
  val wordOnlyTestSample=cleanTestSamples map wordOnlySample

  // word2vec
  val samplePairs=wordOnlyTrainSample.map(x=>x.id->x).cache()
  val reviewWordPairs:RDD[(String, Iterable[String])]=
    samplePairs
      .mapValues(_.review.split(" ").toIterable)

  val word2vec_model=new Word2Vec().fit(reviewWordPairs.values)

  println("finished training!!")
  println(word2vec_model.transform("london"))
  println(word2vec_model.findSynonyms("london", 4))

  def wordFeatures(words:Iterable[String]):Iterable[Vector]=
    words
      .map(w=>Try(word2vec_model.transform(w)))
      .filter(_.isSuccess)
      .map(_.get)

  def avgWordFeatures(word_feature:Iterable[Vector]):Vector=
    word_feature.map(_.toBreeze).reduceLeft(_+_)/word_feature.size.toDouble

  val word_feature_pair=reviewWordPairs mapValues  wordFeatures
  val avg_word_feature_pair=word_feature_pair mapValues avgWordFeatures

  val featuresPair=avg_word_feature_pair join samplePairs mapValues{
    case (feature, Sample(id, review, sentiment))=>LabeledPoint(sentiment.get.toDouble, feature)
  }
  val trainingSet=featuresPair.values

  val Array(x_train,x_test)=trainingSet.randomSplit(Array(0.7,0.3))
  val model=SVMWithSGD.train(x_train, 100)

  val result=model.predict(x_test.map(_.features))

  x_test.map{
    case LabeledPoint(label, features)=>s"$label->${model.predict(features)}}"}
    .take(10)
    .foreach(println(_))

  val accuracy=x_test.filter(x=>x.label==model.predict(x.features))
    .count()
    .toFloat/x_test.count()

    println(s"accuracy is:$accuracy")
}
