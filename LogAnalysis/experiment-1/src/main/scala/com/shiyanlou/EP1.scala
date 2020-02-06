package com.shiyanlou

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming.EsSparkStreaming

object EP1 extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = createSparkConf()
    val spark = buildSparkSession(sparkConf)
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

    //kafka 读参数
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "47.111.19.155:9092", //kafka bootstrap server地址，需要根据实际情况填写
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //key 序列化方式
      "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer", //value 序列化方式
      "group.id" -> "e1", //消费组id
      "auto.offset.reset" -> "latest", //从最新位置开始读
      "enable.auto.commit" -> "true" //自动提交 offset
    )

    // Avro 反序列化模板  同序列化时使用的是同一个
    val schema = "{\n  \"namespace\": \"netflow\",\n  \"type\": \"record\",\n  \"name\": \"netflow\",\n  \"fields\": [\n    {\n      \"name\": \"time_first\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"snmp_in\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"source_as\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"dest_port\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"event_hour\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"dest_as\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"tcp_flags\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"source_port\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"event_date\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"host\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"num_octets\",\n      \"type\": \"int\"\n    },\n    {\n      \"name\": \"num_packets\",\n      \"type\": \"int\"\n    },\n    {\n      \"name\": \"tos\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"timestamp_calculated\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"source_addr\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"dest_addr\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"next_hop\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"time_last\",\n      \"type\": \"int\"\n    },\n    {\n      \"name\": \"protocol\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"snmp_out\",\n      \"type\": \"string\"\n    }\n  ]\n}";

    //从Kafka读数据，反序列化
    val reader = new KafkaReader("netflow_avro", streamingContext, spark, kafkaParams, schema)
    val dStream: DStream[String] = reader.readKafka()

    //ES输出配置
    val esCfg = Map[String, String](
      "es.nodes" -> "47.111.19.155", //ES 地址,需要根据实际情况填写
      "es.nodes.wan.only" -> "true", //是否运行在云服务器等广域网，在此模式下，关闭节点自动发现，只连接已配置节点
      "es.port" -> "9200", //ES 端口
      "es.resource.write" -> "netflow/netflow" //配置输出Index/Mapping
    )

    //输出到ES
    EsSparkStreaming.saveJsonToEs(dStream, esCfg)

    streamingContext.start()
    streamingContext.awaitTermination()
  }


  private def buildSparkSession(sparkConf: SparkConf) = {
    SparkSession
      .builder()
      .appName("EP1")
      .config(sparkConf)
      .getOrCreate()
  }

  private def createSparkConf(): SparkConf = {
    new SparkConf().setAppName("EP1").setMaster("local[4]")
  }

}
