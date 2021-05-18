package com.ddom.taxis;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Ride {

  private static final String COMMA = ",";
  static DateFormat DATE_FORMAT1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static DateFormat DATE_FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  final String medallion;
  final String hack_license;
  final long pickup_datetime;
  final long dropoff_datetime;
  final int trip_time_in_secs;
  final double trip_distance;
  final double pickup_longitude;
  final double pickup_latitude;
  final double dropoff_longitude;
  final double dropoff_latitude;
  final String payment_type;
  final double fare_amount;
  final double surcharge;
  final double mta_tax;
  final double tip_amount;
  final double tolls_amount;
  final double total_amount;

  final boolean parsedOK;

  public Ride(String data) {
    String[] tokens = data.split(COMMA);

    this.medallion = tokens[0];
    this.hack_license = tokens[1];
    this.pickup_datetime = parseDate(tokens[2]);
    this.dropoff_datetime = parseDate(tokens[3]);
    this.trip_time_in_secs = Integer.parseInt(tokens[4]);
    this.trip_distance = Double.parseDouble(tokens[5]);
    this.pickup_longitude = Double.parseDouble(tokens[6]);
    this.pickup_latitude = Double.parseDouble(tokens[7]);
    this.dropoff_longitude = Double.parseDouble(tokens[8]);
    this.dropoff_latitude = Double.parseDouble(tokens[9]);
    this.payment_type = tokens[10];
    this.fare_amount = Double.parseDouble(tokens[11]);
    this.surcharge = Double.parseDouble(tokens[12]);
    this.mta_tax = Double.parseDouble(tokens[13]);
    this.tip_amount = Double.parseDouble(tokens[14]);
    this.tolls_amount = Double.parseDouble(tokens[15]);
    this.total_amount = Double.parseDouble(tokens[16]);

    this.parsedOK = this.pickup_datetime > 0 && this.dropoff_datetime > 0;
  }

  private static long parseDate(String date) {
    try {
      return DATE_FORMAT1.parse(date).getTime();
    } catch (Exception e) {
      return -1;
    }
  }

  synchronized public String toCsvLine(Ride ref, int speed) {
    long pickup_datetime = ref.pickup_datetime + (this.pickup_datetime - ref.pickup_datetime) / speed;
    long dropoff_datetime = ref.dropoff_datetime + (this.dropoff_datetime - ref.dropoff_datetime) / speed;
    String pickup_datetime_str = DATE_FORMAT2.format(new Date(pickup_datetime));
    String dropoff_datetime_str = DATE_FORMAT2.format(new Date(dropoff_datetime));
    return getLineStr(pickup_datetime_str, dropoff_datetime_str);
  }

  @Override
  synchronized public String toString() {
    String pickup_datetime_str = DATE_FORMAT2.format(new Date(pickup_datetime));
    String dropoff_datetime_str = DATE_FORMAT2.format(new Date(dropoff_datetime));
    return getLineStr(pickup_datetime_str, dropoff_datetime_str);
  }

  private String getLineStr(String pickup_datetime_str, String dropoff_datetime_str) {
    return medallion + COMMA
        + hack_license + COMMA
        + pickup_datetime_str + COMMA
        + dropoff_datetime_str + COMMA
        + trip_time_in_secs + COMMA
        + trip_distance + COMMA
        + pickup_longitude + COMMA
        + pickup_latitude + COMMA
        + dropoff_longitude + COMMA
        + dropoff_latitude + COMMA
        + payment_type + COMMA
        + fare_amount + COMMA
        + surcharge + COMMA
        + mta_tax + COMMA
        + tip_amount + COMMA
        + tolls_amount + COMMA
        + total_amount;
  }
}