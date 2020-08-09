package com.test.tcp;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Application {
  
  public static void main(String args[]) {

    
    String topicName = "testTopic";
    Properties props = new Properties();

    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    
    System.out.println(props);

    Producer<String, String> producer = new KafkaProducer<String, String>(props);

    for (int i = 0; i < 10; i++) {
      System.out.println("Sending Message : " + i );
      producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i)));
    }
    System.out.println("Message sent successfully");
      producer.close();
    System.out.println("Producer Closed Successfully");
  
  }

}
