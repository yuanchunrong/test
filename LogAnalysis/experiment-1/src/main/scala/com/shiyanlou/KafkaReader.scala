package com.shiyanlou

import java.util.Base64

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DatumReader, DecoderFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

// 继承 Logging 类和 Java序列化类
class KafkaReader(topicName: String, // 主题名
                  streamingContext: StreamingContext,
                  sparkSession: SparkSession,
                  kafkaConfig: Map[String, String],
                  @transient // 标识变量是瞬时的，使成员变量不会被序列化
                  strSchema: String) extends Logging with java.io.Serializable {

  def readKafka(): DStream[String] = {
    // 接收数据，转化为 spark streaming 中的数据结构 DStream
    val recordDStream = KafkaUtils.createDirectStream[String, Array[Byte]](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Array[Byte]](Array(topicName), kafkaConfig)
    )

    val parser = new Schema.Parser()
    val schema = parser.parse(strSchema)
    val structType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]

    println(structType.prettyJson)

    @transient
    val str = strSchema

    val dStream = recordDStream.map(consumerRecord => {
      var defaultDecoder: BinaryDecoder = null
      val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]((new Schema.Parser).parse(str))
      val message: Array[Byte] = consumerRecord.value
      defaultDecoder = DecoderFactory.get.binaryDecoder(Base64.getDecoder.decode(message), defaultDecoder)
      //反序列化
      val record: GenericRecord = reader.read(null, defaultDecoder)
      println("msg => " + record.toString)
      record.toString
    })
    dStream
  }
}
