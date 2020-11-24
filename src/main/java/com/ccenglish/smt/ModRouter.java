package com.ccenglish.smt;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class ModRouter<R extends ConnectRecord<R>> implements Transformation<R>, AutoCloseable {
//  public static final ConfigDef CONFIG_DEF;
//  private String key;
//  private Integer mod;
//
//  public ModRouter() {
//  }
//
//  public void configure(Map<String, ?> props) {
//    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
//    this.key = config.getString("key");
//    this.mod = config.getInt("mod");
//  }
//
//  public R apply(R record) {
//    if (!(record.value() instanceof Struct)) {
//      throw new DataException("record not struct " + record);
//    }
//    Struct value = (Struct)record.value();
//    if (value.schema().field(key) == null) {
//      throw new DataException(key + " missing on record: " + record);
//    }
//    Long p = null;
//    try {
//      p = (Long)value.get(key);
//    } catch (Throwable e) {
//      throw new DataException(key + " is not number: " + record);
//    }
//    String newTopic = record.topic() + "-" + (p % mod);
//    return record.newRecord(newTopic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp(), record.headers());
//  }
//
//  public void close() {
//    this.timestampFormat = null;
//  }
//
//  public ConfigDef config() {
//    return CONFIG_DEF;
//  }
//
//  static {
//    CONFIG_DEF = (new ConfigDef()).define("key", Type.STRING, "id", Importance.HIGH, "field")
//            .define("number", Type.INT, 10, Importance.HIGH, "mod");
//  }
//
//  private interface ConfigName {
//    String KEY = "key";
//    String NUMBER = "number";
//  }
}

