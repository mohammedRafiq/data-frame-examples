package com.dsm.rdd

import com.dsm.model.{Demographic, Finance, Course}
import org.apache.spark.sql.SparkSession

/**
  * Count: Swiss students who have debt & financial dependents:
  * 1. Inner join first
  * 2. Filter to select people in Switzerland
  * 3. Filter to select people with debt & financial dependents
  *
  */
object ScholashipRecipientJoinFilter {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAZP5OISPJ2QGSQDZE")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val demographicsRDD = sparkSession.sparkContext.textFile("s3n://rafiqdata/demographic.csv")
    val financesRDD = sparkSession.sparkContext.textFile("s3n://rafiqdata/finances.csv")
    val coursesRDD = sparkSession.sparkContext.textFile("s3n://rafiqdata/course.csv")

//    println("# of records = " + demographicsRDD.count())

    val demographicsPairedRdd = demographicsRDD.map(record => record.split(","))
      .map(record =>
        Demographic(record(0).toInt,
          record(1).toInt,
          record(2).toBoolean,
          record(3),
          record(4),
          record(5).toBoolean,
          record(6).toBoolean,
          record(7).toInt
        )
      ).map(demographic => (demographic.id, demographic))      //Pair RDD, (id, demographics)

    val financesPairedRdd = financesRDD.map(record => record.split(","))
      .map(record => Finance(
        record(0).toInt,
        record(1).toBoolean,
        record(2).toBoolean,
        record(3).toBoolean,
        record(4).toInt
      )
      ).map(finance => (finance.id, finance))                  //Pair RDD, (id, finances)

    val courses = coursesRDD.map(record => record.split(","))
      .map(record => Course(record(0).toInt, record(1)))
      .map(course => (course.id, course))                  //Pair RDD, (id, finances)

    demographicsPairedRdd.join(financesPairedRdd)
          .filter(p => p._2._1.country == "Switzerland"
            && p._2._2.hasFinancialDependents
            && p._2._2.hasDebt)
      .map(p => (p._2._1.courseId, (p._2._1, p._2._2)))         // Further joining with Course Data set
      .join(courses)
      .map(p => (p._2._1._1.id, (p._2._1._1, p._2._1._2, p._2._2)))
      .foreach(println)

    sparkSession.close()
  }
}