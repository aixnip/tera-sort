import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

object SSort {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.exit(0)
    }

    val inputFile = args(0)
    val outputFile = args(1)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("TeraSort")
      .set("textinputformat.record.delimiter", "\n")

    val sc = new SparkContext()
    val line = sc.textFile(inputFile)
    val dataset = line.map(byline => (byline.substring(0, 10), byline.substring(10))).sortByKey().map{case(key, value) => key + value}
    dataset.saveAsTextFile(outputFile)
  }
}
