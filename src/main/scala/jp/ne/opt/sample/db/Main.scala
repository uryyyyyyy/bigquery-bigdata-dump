package jp.ne.opt.sample.db

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{Framing, Source}
import akka.stream.{ActorMaterializer, Graph, SourceShape}
import akka.util.ByteString
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery._
import com.google.cloud.storage.StorageOptions

import scala.concurrent.Await
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {
    val projectId = "teak-trainer-806"
    val bucketName = "teak-trainer-806-ml"
    val bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService
    val destination = TableId.of("aaa", "bbb")
    queryToTable(bigquery, destination)
    println("queryToTable fin")
    dumpFromBq(bigquery, destination, bucketName)
    println("dumpFromBq fin")
    readFromGCSStreaming(bucketName, "bq-dump")
    //    OoMになるかもの例
    //    readFromGCS(bucketName, "bq-dump")
  }

  def queryToTable(bigquery: BigQuery, destination: TableId): Unit = {
    val queryConfig = QueryJobConfiguration.newBuilder(
      "SELECT * from `bigquery-public-data.stackoverflow.posts_questions` limit 100000"
    ).setUseLegacySql(false)
      .setDestinationTable(destination)
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .setAllowLargeResults(true)
      .build

    var queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build)
    queryJob = queryJob.waitFor()
  }

  def dumpFromBq(bigquery: BigQuery, destination: TableId, bucket: String): Unit = {
    val tempFileURL = s"gs://${bucket}/bq-dump"
    val queryConfig2 = ExtractJobConfiguration
      .newBuilder(destination, tempFileURL)
      .setFormat("CSV")
      .build

    var queryJob2 = bigquery.create(JobInfo.newBuilder(queryConfig2).build)
    queryJob2 = queryJob2.waitFor()
  }

  def readFromGCS(bucket: String, filePath: String): Unit = {
    val storage = StorageOptions.newBuilder().setProjectId("teak-trainer-806").build().getService
    val aa = storage.readAllBytes(bucket, filePath)
    println(aa.size)
  }

  def readFromGCSStreaming(bucket: String, filePath: String): Unit = {
    val storage = StorageOptions.newBuilder().setProjectId("teak-trainer-806").build().getService
    val reader = storage.reader(bucket, filePath)

    implicit val system = ActorSystem("gc-example")
    implicit val materializer = ActorMaterializer()
    val stage: Graph[SourceShape[ByteString], NotUsed] = new GoogleStorageGraphStage(reader)

    val mySource = Source.fromGraph(stage)

    println("start")
    val aa = mySource
      // CSVでquote中にquoteがあると以下のエラーが。
      // quote中のquoteを別の文字に置換して後で直したりする必要がありそう。
      // akka.stream.alpakka.csv.MalformedCsvException: wrong escaping at 10:313, only escape or quote may be escaped within quotes
      //      .via(CsvParsing.lineScanner())
      //      .via(CsvToMap.toMap())
      //      .map(_.mapValues(_.utf8String))
      //      .runForeach(println)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 100000))
      .map(_.utf8String)
      .map(_ => 1)
      .reduce((a, b) => a + b)
      .runForeach(println)
    Await.result(aa, Duration.Inf)
    system.terminate()
  }
}