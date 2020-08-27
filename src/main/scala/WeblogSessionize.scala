package com.fik.weblog.sessionize


import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, TimestampType}

case class web_log (timestamp: String,
                 elb: String,
                 client_ip: String,
                 client_port: String,
                 backend_port: String,
                 request_processing_time: String,
                 backend_processing_time: String,
                 response_processing_time: String,
                 elb_status_code: String,
                 backend_status_code: String,
                 received_bytes: String,
                 sent_bytes: String,
                 request_type: String,
                 request_url: String,
                 user_agent: String,
                 ssl_cipher: String,
                 ssl_protocol: String);

object WeblogSessionize {

  val output_path = "result"
  val log_regex = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r
  val file_name = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
  def main(args: Array[String]): Unit = {

    // udf for check if its new session or not
    val is_new_session = udf((duration: Long) => {
      if (duration > 900000) 1
      else 0
    })

    // udf for timestamp diff in milliseconds
    val timestamp_diff = udf((startTime: Timestamp, endTime: Timestamp) => {
      (startTime.getTime() - endTime.getTime())
    })

    // ------------- PREPARE AND PARSED WEB LOG DATA -----------------
    // init spark session for running locally
    // load  data and parse out visits, sorted by ip and time

    val spark_session = SparkSession.builder
      .master("local[*]")
      .config("spark.driver.memory","10g")
      .config("spark.driver.host","127.0.0.1")
      .getOrCreate()

    val spark_context = spark_session.sparkContext
    spark_context.setLogLevel("ERROR")
    // converting text file into rdd
    val raw_log = spark_context.textFile(file_name)

    // parse raw rdd into log object
    val parsed_logs = raw_log.filter(line => line.matches(log_regex.toString)).map(line => parseLog(line));

    // convert rdd into dataframe
    val web_logs_df = spark_session.createDataFrame(parsed_logs)

    // cast each column to appropriate data type
    val web_logs_df_formatted = web_logs_df
        .withColumn("timestamp", col("timestamp").cast(TimestampType))
        .withColumn("request_processing_time", col("request_processing_time").cast(DoubleType))
        .withColumn("backend_processing_time", col("backend_processing_time").cast(DoubleType))
        .withColumn("response_processing_time", col("response_processing_time").cast(DoubleType))


    // set window partition spec when we want to create session id
    val window_partition_spec = Window.partitionBy(col("client_ip"),col("user_agent")).orderBy("timestamp")


    // Assumptions: Sessions ONLY end when the user inactivity exceeds The inactivity threshold which is to 900000 milliseconds (15 mins)

    // ------------- Q1 :  SESSIONIZE  WEB LOG BY IP WITH ASSUMED 15 MINUTE WINDOW -----------------

    // get previous offset timestamp based on client ip and user agent
    val prev_session_log = web_logs_df_formatted.withColumn("prev_timestamp", lag("timestamp",1).over(window_partition_spec)).orderBy("timestamp")

    // replace null value for previous offset timestamp
    val web_log_prev_session_cleaned = prev_session_log.withColumn("prev_timestamp_cleaned", coalesce(col("prev_timestamp"), col("timestamp")))

    // calculate duration between next timestamp and previous timestamp
    val web_log_with_duration = web_log_prev_session_cleaned.withColumn("duration", timestamp_diff(col("timestamp"), col("prev_timestamp_cleaned")))

    // if duration is more than 5 min , we will consider it as new session(1) else not new session(0)
    val web_log_with_session_info = web_log_with_duration.withColumn("is_new_session", is_new_session(col("duration")))

    // add window index
    val web_log_with_window = web_log_with_session_info.withColumn("window_ids",sum("is_new_session").over(window_partition_spec).cast("string"))

    // create new dataframe with window index column
    val web_log_with_session_id =  web_log_with_window
      .withColumn("session_id",concat(col("user_agent"),lit("_"),col("client_ip"),lit("_"),col("window_ids")))
      .select(
        col("client_ip"),
        col("session_id"),
        col("duration"),
        col("request_url"),
        col("user_agent")).cache

    web_log_with_session_id.show(false)
    //web_log_with_session_id.write.csv(output_path+"/web_log_with_session_id")

    // ------------- Q2 : Determine the average session time ----------------- //
    web_log_with_session_id.select(avg("duration").alias("average session time")).show(false)

    //  ------------- Q3 : Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session  ----------------- //
    web_log_with_session_id.select("session_id","request_url").groupBy("session_id").agg(countDistinct("request_url").as("hits_url")).show(false)

    //  ------------- Q4 : Find the most engaged users, ie the IPs with the longest session times  ----------------- //
     web_log_with_session_id.groupBy(col("client_ip"), col("user_agent")).agg(sum("duration").alias("total_session")).orderBy(col("total_session").desc).limit(10).show()
  }


  def parseLog(line: String):web_log = {
    val log_regex(timestamp, elb, client_port, backend_port, request_processing_time, backend_processing_time, response_processing_time, elb_status_code, backend_status_code, received_bytes, sent_byte, request, user_agent, ssl_cipher, ssl_protocol) = line;
    return web_log(
      timestamp,
      elb,
      client_port.split(":")(0),
      client_port.split(":")(1),
      backend_port,
      request_processing_time,
      backend_processing_time,
      response_processing_time,
      elb_status_code,
      backend_status_code,
      received_bytes,
      sent_byte,
      request.split(" ")(0),
      request.split(" ")(1),
      user_agent,
      ssl_cipher,
      ssl_protocol)
  }
}
