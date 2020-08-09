package com.test.tcp;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaUtils implements Closeable {
  
  Producer<String, String> producer;
  
  void initialiseKafkaProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    this.producer = new KafkaProducer<String, String>(props);
  }
  
  boolean postMessageToKafka(String topicName, String message) {
    String topic = ((topicName == null) || (topicName.trim().equals(""))) ? "testTopic" : topicName;
    producer.send(new ProducerRecord<String, String>(topic, message));
    return true;
  }

  @Override
  public void close() throws IOException {
    producer.close();
  }
  
}
