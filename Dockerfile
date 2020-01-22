FROM debezium/connect
COPY build/libs/*.jar /kafka/connect/debezium-connector-mysql
