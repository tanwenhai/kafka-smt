package com.ccenglish.smt;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.transforms.SmtManager;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.Strings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class TransformState<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformState.class);

  private static final String PURPOSE = "source field insertion";
  private static final int SCHEMA_CACHE_SIZE = 64;
  private static final Pattern FIELD_SEPARATOR = Pattern.compile("\\.");

  private boolean dropTombstones;
  private ExtractNewRecordStateConfigDefinition.DeleteHandling handleDeletes;
  private List<TransformState.FieldReference> additionalHeaders;
  private List<TransformState.FieldReference> additionalFields;
  private String routeByField;
  private final ExtractField<R> afterDelegate = new ExtractField.Value<R>();
  private final ExtractField<R> beforeDelegate = new ExtractField.Value<R>();
  private final InsertField<R> removedDelegate = new InsertField.Value<R>();
  private final InsertField<R> updatedDelegate = new InsertField.Value<R>();
  private BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache;
  private SmtManager<R> smtManager;

  @Override
  public void configure(final Map<String, ?> configs) {
    final Configuration config = Configuration.from(configs);
    smtManager = new SmtManager<>(config);

    final Field.Set configFields = Field.setOf(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES, ExtractNewRecordStateConfigDefinition.HANDLE_DELETES);
    if (!config.validateAndRecord(configFields, LOGGER::error)) {
      throw new ConnectException("Unable to validate config.");
    }

    dropTombstones = config.getBoolean(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES);
    handleDeletes = ExtractNewRecordStateConfigDefinition.DeleteHandling.parse(config.getString(ExtractNewRecordStateConfigDefinition.HANDLE_DELETES));

    String addFieldsPrefix = config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS_PREFIX);
    String addHeadersPrefix = config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS_PREFIX);
    additionalFields = TransformState.FieldReference.fromConfiguration(addFieldsPrefix, config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS));
    additionalHeaders = TransformState.FieldReference.fromConfiguration(addHeadersPrefix, config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS));

    String routeFieldConfig = config.getString(ExtractNewRecordStateConfigDefinition.ROUTE_BY_FIELD);
    routeByField = routeFieldConfig.isEmpty() ? null : routeFieldConfig;

    Map<String, String> delegateConfig = new LinkedHashMap<>();
    delegateConfig.put("field", "before");
    beforeDelegate.configure(delegateConfig);

    delegateConfig = new HashMap<>();
    delegateConfig.put("field", "after");
    afterDelegate.configure(delegateConfig);

    delegateConfig = new HashMap<>();
    delegateConfig.put("static.field", ExtractNewRecordStateConfigDefinition.DELETED_FIELD);
    delegateConfig.put("static.value", "true");
    removedDelegate.configure(delegateConfig);

    delegateConfig = new HashMap<>();
    delegateConfig.put("static.field", ExtractNewRecordStateConfigDefinition.DELETED_FIELD);
    delegateConfig.put("static.value", "false");
    updatedDelegate.configure(delegateConfig);

    schemaUpdateCache = new BoundedConcurrentHashMap<>(SCHEMA_CACHE_SIZE);
  }

  @Override
  public R apply(final R record) {
    if (record.value() == null) {
      if (dropTombstones) {
        LOGGER.trace("Tombstone {} arrived and requested to be dropped", record.key());
        return null;
      }
      if (!additionalHeaders.isEmpty()) {
        Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
        headersToAdd.forEach(h -> record.headers().add(h));
      }
      return record;
    }

    if (!smtManager.isValidEnvelope(record)) {
      return record;
    }

    if (!additionalHeaders.isEmpty()) {
      Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
      headersToAdd.forEach(h -> record.headers().add(h));
    }

    R newRecord = afterDelegate.apply(record);
    if (newRecord.value() == null) {
      if (routeByField != null) {
        Struct recordValue = requireStruct(record.value(), "Read record to set topic routing for DELETE");
        String newTopicName = recordValue.getStruct("before").getString(routeByField);
        newRecord = setTopic(newTopicName, newRecord);
      }

      // Handling delete records
      switch (handleDeletes) {
        case DROP:
          LOGGER.trace("Delete message {} requested to be dropped", record.key());
          return null;
        case REWRITE:
          LOGGER.trace("Delete message {} requested to be rewritten", record.key());
          R oldRecord = beforeDelegate.apply(record);
          oldRecord = addFields(additionalFields, record, oldRecord);

          return removedDelegate.apply(oldRecord);
        default:
          return makeDeleteRecord(record);
      }
    }
    else {
      // Add on any requested source fields from the original record to the new unwrapped record
      if (routeByField != null) {
        Struct recordValue = requireStruct(newRecord.value(), "Read record to set topic routing for CREATE / UPDATE");
        String newTopicName = recordValue.getString(routeByField);
        newRecord = setTopic(newTopicName, newRecord);
      }

      // Handling insert and update records
      switch (handleDeletes) {
        case REWRITE:
          newRecord = addFields(additionalFields, record, newRecord);
          LOGGER.trace("Insert/update message {} requested to be rewritten", record.key());
          return updatedDelegate.apply(newRecord);
        default:
          newRecord = makeUpdateRecord(record);
          if (newRecord != null && newRecord.value() != null) {
            newRecord = addFields(additionalFields, record, newRecord);
          }
          return newRecord;
      }
    }
  }

  private R makeUpdateRecord(R record) {
    R beforeRecord = beforeDelegate.apply(record);
    R afterRecord = afterDelegate.apply(record);
    String softDeleteFieldName = "is_del";
    short softDeleteValue = (short)1;
    if (beforeRecord.value() == null) {
      record.headers().remove(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
      record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, Envelope.Operation.CREATE.code());
      Struct afterValue = requireStruct(afterRecord.value(), "Read after value for CREATE");
      record = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), afterRecord.valueSchema(), afterValue, record.timestamp(), record.headers());
      if (afterValue.schema().field(softDeleteFieldName) != null) {
        Object softDeleteFieldObject = afterValue.get(softDeleteFieldName);
        if (softDeleteFieldObject instanceof Number) {
          if (((Number) softDeleteFieldObject).shortValue() == softDeleteValue) {
            // 新增的记录是软删除
            LOGGER.warn("ignore soft delete record create {}", record);
            return null;
////            record.headers().addStruct("__before", afterValue);
//            record.headers().remove(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
//            record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, Envelope.Operation.DELETE.code());
//            record = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, null, record.timestamp(), record.headers());
          }
        }
      }
      return record;
    }
    Struct beforeValue = requireStruct(beforeRecord.value(), "Read before value for UPDATE");
    Struct afterValue = requireStruct(afterRecord.value(), "Read after value for UPDATE");

    record.headers().addStruct("__before", beforeValue);
    record.headers().remove(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
    record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, Envelope.Operation.UPDATE.code());
    record = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), afterRecord.valueSchema(), afterValue, record.timestamp(), record.headers());

    if (beforeValue.schema().field(softDeleteFieldName) != null || afterValue.schema().field(softDeleteFieldName) != null) {
      Object beforeSoftDeleteObject = beforeValue.get(softDeleteFieldName);
      Object afterSoftDeleteObject = afterValue.get(softDeleteFieldName);
      if (beforeSoftDeleteObject instanceof Number && afterSoftDeleteObject instanceof Number) {
        short beforeSoftDeleteValue = ((Number) beforeSoftDeleteObject).shortValue();
        short afterSoftDeleteValue = ((Number) afterSoftDeleteObject).shortValue();
        if (beforeSoftDeleteValue == softDeleteValue && afterSoftDeleteValue == softDeleteValue) {
          // 更新前后都是软删除
          LOGGER.warn("ignore soft delete record update {}", record);
          return null;
        } else if (beforeSoftDeleteValue == softDeleteValue) {
          // 软删除记录恢复了
          record.headers().addStruct("__before", beforeValue);
          record.headers().remove(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
          record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, Envelope.Operation.CREATE.code());
          record = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), afterRecord.valueSchema(), afterValue, record.timestamp(), record.headers());
        } else if (afterSoftDeleteValue == softDeleteValue) {
          // 记录被软删除了
          record.headers().addStruct("__before", beforeValue);
          record.headers().remove(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
          record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, Envelope.Operation.DELETE.code());
          record = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), afterRecord.valueSchema(), afterValue, record.timestamp(), record.headers());
        }
      }
    }
    record = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), afterRecord.valueSchema(), afterValue, record.timestamp(), record.headers());

    return record;
  }

  private R makeDeleteRecord(R record) {
    R beforeRecord = beforeDelegate.apply(record);
    R afterRecord = afterDelegate.apply(record);
    if (beforeRecord.value() != null) {
      afterRecord.headers().addStruct("__before", requireStruct(beforeRecord.value(), "Read record to set topic header __before for DELETE"));
    }
    record.headers().remove(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
    afterRecord.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY,
            Envelope.Operation.DELETE.code());
    return afterRecord;
  }

  private R setTopic(String updatedTopicValue, R record) {
    String topicName = updatedTopicValue == null ? record.topic() : updatedTopicValue;

    return record.newRecord(
            topicName,
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            record.value(),
            record.timestamp());
  }

  /**
   * Create an Headers object which contains the headers to be added.
   */
  private Headers makeHeaders(List<TransformState.FieldReference> additionalHeaders, Struct originalRecordValue) {
    Headers headers = new ConnectHeaders();

    for (TransformState.FieldReference fieldReference : additionalHeaders) {
      // add "d" operation header to tombstone events
      if (originalRecordValue == null) {
        if (Envelope.FieldName.OPERATION.equals(fieldReference.field)) {
          headers.addString(fieldReference.newFieldName, Envelope.Operation.DELETE.code());
        }
        continue;
      }
      headers.add(fieldReference.getNewFieldName(), fieldReference.getValue(originalRecordValue),
              fieldReference.getSchema(originalRecordValue.schema()));
    }

    return headers;
  }

  private R addFields(List<TransformState.FieldReference> additionalFields, R originalRecord, R unwrappedRecord) {
    final Struct value = requireStruct(unwrappedRecord.value(), PURPOSE);
    Struct originalRecordValue = (Struct) originalRecord.value();

    Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(),
            s -> makeUpdatedSchema(additionalFields, value.schema(), originalRecordValue));

    // Update the value with the new fields
    Struct updatedValue = new Struct(updatedSchema);
    for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
      updatedValue.put(field.name(), value.get(field));

    }

    for (TransformState.FieldReference fieldReference : additionalFields) {
      updatedValue = updateValue(fieldReference, updatedValue, originalRecordValue);
    }

    return unwrappedRecord.newRecord(
            unwrappedRecord.topic(),
            unwrappedRecord.kafkaPartition(),
            unwrappedRecord.keySchema(),
            unwrappedRecord.key(),
            updatedSchema,
            updatedValue,
            unwrappedRecord.timestamp());
  }

  private Schema makeUpdatedSchema(List<TransformState.FieldReference> additionalFields, Schema schema, Struct originalRecordValue) {
    // Get fields from original schema
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (org.apache.kafka.connect.data.Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    // Update the schema with the new fields
    for (TransformState.FieldReference fieldReference : additionalFields) {
      builder = updateSchema(fieldReference, builder, originalRecordValue.schema());
    }

    return builder.build();
  }

  private SchemaBuilder updateSchema(TransformState.FieldReference fieldReference, SchemaBuilder builder, Schema originalRecordSchema) {
    return builder.field(fieldReference.getNewFieldName(), fieldReference.getSchema(originalRecordSchema));
  }

  private Struct updateValue(TransformState.FieldReference fieldReference, Struct updatedValue, Struct struct) {
    return updatedValue.put(fieldReference.getNewFieldName(), fieldReference.getValue(struct));
  }

  @Override
  public ConfigDef config() {
    final ConfigDef config = new ConfigDef();
    Field.group(config, null, ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES,
            ExtractNewRecordStateConfigDefinition.HANDLE_DELETES, ExtractNewRecordStateConfigDefinition.ADD_FIELDS,
            ExtractNewRecordStateConfigDefinition.ADD_HEADERS,
            ExtractNewRecordStateConfigDefinition.ROUTE_BY_FIELD);
    return config;
  }

  @Override
  public void close() {
    beforeDelegate.close();
    afterDelegate.close();
    removedDelegate.close();
    updatedDelegate.close();
  }

  /**
   * Represents a field that should be added to the outgoing record as a header
   * attribute or struct field.
   */
  private static class FieldReference {

    /**
     * The struct ("source", "transaction") hosting the given field, or {@code null} for "op" and "ts_ms".
     */
    private final String struct;

    /**
     * The simple field name.
     */
    private final String field;

    /**
     * The prefix for the new field name.
     */
    private final String prefix;
    /**
     * The name for the outgoing attribute/field, e.g. "__op" or "__source_ts_ms" when the prefix is "__"
     */
    private final String newFieldName;

    private FieldReference(String prefix, String field) {
      this.prefix = prefix;
      String[] parts = FIELD_SEPARATOR.split(field);

      if (parts.length == 1) {
        this.struct = determineStruct(parts[0]);
        this.field = parts[0];
        this.newFieldName = prefix + field;
      }
      else if (parts.length == 2) {
        this.struct = parts[0];

        if (!(this.struct.equals(Envelope.FieldName.SOURCE) || this.struct.equals(Envelope.FieldName.TRANSACTION))) {
          throw new IllegalArgumentException("Unexpected field name: " + field);
        }

        this.field = parts[1];
        this.newFieldName = prefix + this.struct + "_" + this.field;
      }
      else {
        throw new IllegalArgumentException("Unexpected field name: " + field);
      }
    }

    /**
     * Determines the struct hosting the given unqualified field.
     */
    private static String determineStruct(String simpleFieldName) {
      if (simpleFieldName.equals(Envelope.FieldName.OPERATION) || simpleFieldName.equals(Envelope.FieldName.TIMESTAMP)) {
        return null;
      }
      else if (simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY) ||
              simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY) ||
              simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY)) {
        return Envelope.FieldName.TRANSACTION;
      }
      else {
        return Envelope.FieldName.SOURCE;
      }
    }

    static List<TransformState.FieldReference> fromConfiguration(String fieldPrefix, String addHeadersConfig) {
      if (Strings.isNullOrEmpty(addHeadersConfig)) {
        return Collections.emptyList();
      }
      else {
        return Arrays.stream(addHeadersConfig.split(","))
                .map(String::trim)
                .map(field -> new TransformState.FieldReference(fieldPrefix, field))
                .collect(Collectors.toList());
      }
    }

    String getNewFieldName() {
      return newFieldName;
    }

    Object getValue(Struct originalRecordValue) {
      Struct parentStruct = struct != null ? (Struct) originalRecordValue.get(struct) : originalRecordValue;

      // transaction is optional; e.g. not present during snapshotting atm.
      return parentStruct != null ? parentStruct.get(field) : null;
    }

    Schema getSchema(Schema originalRecordSchema) {
      Schema parentSchema = struct != null ? originalRecordSchema.field(struct).schema() : originalRecordSchema;

      org.apache.kafka.connect.data.Field schemaField = parentSchema.field(field);

      if (schemaField == null) {
        throw new IllegalArgumentException("Unexpected field name: " + field);
      }

      return SchemaUtil.copySchemaBasics(schemaField.schema()).optional().build();
    }
  }
}
