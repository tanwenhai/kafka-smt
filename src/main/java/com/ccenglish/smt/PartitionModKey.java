package com.ccenglish.smt;

import io.debezium.config.Configuration;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class PartitionModKey<R extends ConnectRecord<R>> implements Transformation<R> {
  private Configuration config;

  @Override
  public void configure(Map<String, ?> configs) {
    config = Configuration.from(configs);
  }
  @Override
  public R apply(R record) {
    if (record.value() instanceof Struct) {
      String key = config.getString("key");
      Integer mod = config.getInteger("mod");

      Struct struct = (Struct)record.value();
      Object partitionKey = struct.get(key);
      if (partitionKey instanceof Number) {
        int kafkaPartition = calculatePartition(((Number) partitionKey).intValue(), mod);
        record = record.newRecord(record.topic(), kafkaPartition, record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp(), record.headers());
      }
    }

    return record;
  }

  private int calculatePartition(int key, int mod) {
    return key % mod;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {

  }
}
