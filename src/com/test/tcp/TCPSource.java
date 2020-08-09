package com.test.tcp;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPSource {
  
  public static void main(String args[]) {
    try (KafkaUtils kafkaUtils = new KafkaUtils()) {
      kafkaUtils.initialiseKafkaProducer();
      while (true) {
        try (ServerSocket server = new ServerSocket(5000);
            Socket socket = server.accept();
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
          System.out.println("New TCP session started");

          String line = "";
          try {
            while (true) {
              line = in.readUTF();
              System.out.println(line);
              kafkaUtils.postMessageToKafka("", line);
            }
          } catch (IOException i) {
            System.out.println("Unable to read line from TCP Client | Client closed connection");
            System.out.println(i);
          }
        } catch (Exception e) {
          System.out.println("Socket Closed");
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      System.out.println("Error While Connecting to Kafka");
    }
  }
}
