package com.ddom.taxis;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Prod {

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC = "debs";
  private static final String DATASET = "sorted_data.csv.gz";

  private static final int MAX_CAPACITY = 512;
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private final BlockingQueue<Ride> queue = new ArrayBlockingQueue<>(MAX_CAPACITY);

  public void read(String dataset) {
    try( BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(dataset))))) {
      br.lines().filter(line -> line.length() > 4)
          .map(Ride::new)
          .filter(r -> r.parsedOK)
          .forEach(this::putInQueue);
    } catch(Exception e) {
      e.printStackTrace();
    }
    System.exit(0);
  }

  public void publish(int speed) throws Exception {
    Logger.getAnonymousLogger().log(Level.INFO, "Connected to Kafka, starting to send events...");

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Ride first = queue.take();
    int rideCounter = 0;
    try (Producer<String, String> producer = new KafkaProducer<>(props)) {
      System.out.println("Producer started, publishing dataset...");
      Ride previous = first;
      while(true) {
        Ride ride = queue.take();
        String key = ride.medallion.substring(0, 4);
        long delay = (ride.dropoff_datetime - previous.dropoff_datetime) / speed ;
        sleep(delay);
        ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, key, ride.toCsvLine(first, speed));
        producer.send(data);
        previous = ride;
        System.out.printf("Lines: %d - Time: %s\r", rideCounter++, DATE_FORMAT.format( new Date( ride.dropoff_datetime )));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "OFF");
    int speed = args.length == 0 ? 100 : Integer.parseInt( args[0]);
    Prod publisher = new Prod();
    new Thread( () -> publisher.read(DATASET)).start();
    publisher.publish(speed);
  }

  private void putInQueue(Ride r) {
    try {
      queue.put(r);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void sleep(long ms) {
    try {
      if(ms > 0)
        Thread.sleep(ms);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}