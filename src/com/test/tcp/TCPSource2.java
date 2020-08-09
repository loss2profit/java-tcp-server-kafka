package com.test.tcp;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPSource2 {

  public static void main(String args[]) {
    ServerSocket server = null;
    Socket socket = null;
    DataInputStream in = null;

    try (KafkaUtils kafkaUtils = new KafkaUtils()) {
      kafkaUtils.initialiseKafkaProducer();
      server = new ServerSocket(5000);
      System.out.println("Server started");
      System.out.println("Waiting for a client ...");
      socket = server.accept();
      System.out.println("Client accepted");
      in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
      String line = "";
      while (true) {
        try {
          line = in.readUTF();
          System.out.println(line);
          kafkaUtils.postMessageToKafka("", line);
        } catch (IOException i) {
          System.out.println(i);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      System.out.println("Closing connection");
      try {
        socket.close();
        in.close();
        server.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
