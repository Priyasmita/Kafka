// pom.xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.0</version>
        <relativePath/>
    </parent>
    
    <groupId>com.example</groupId>
    <artifactId>kafka-oauth-consumer</artifactId>
    <version>1.0.0</version>
    <name>kafka-oauth-consumer</name>
    
    <properties>
        <java.version>17</java.version>
    </properties>
    
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro</artifactId>
            <version>1.11.3</version>
        </dependency>
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-avro-serializer</artifactId>
            <version>7.5.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </dependency>
    </dependencies>
    
    <repositories>
        <repository>
            <id>confluent</id>
            <url>https://packages.confluent.io/maven/</url>
        </repository>
    </repositories>
    
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>

// src/main/java/com/example/kafka/KafkaOAuthConsumerApplication.java
package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaOAuthConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaOAuthConsumerApplication.class, args);
    }
}

// src/main/java/com/example/kafka/config/KafkaConfig.java
package com.example.kafka.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.schema-registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.security.protocol}")
    private String securityProtocol;

    @Value("${kafka.sasl.mechanism}")
    private String saslMechanism;

    @Value("${kafka.sasl.jaas.config}")
    private String saslJaasConfig;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.oauth.client-id}")
    private String oauthClientId;

    @Value("${kafka.oauth.client-secret}")
    private String oauthClientSecret;

    @Value("${kafka.oauth.token-endpoint}")
    private String tokenEndpoint;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        // Basic Kafka configuration
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        
        // Schema Registry configuration
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        
        // Security configuration
        props.put("security.protocol", securityProtocol);
        props.put("sasl.mechanism", saslMechanism);
        
        // OAuth2 SASL configuration
        String jaasConfig = String.format(
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
            "oauth.client.id=\"%s\" " +
            "oauth.client.secret=\"%s\" " +
            "oauth.token.endpoint.uri=\"%s\";",
            oauthClientId, oauthClientSecret, tokenEndpoint
        );
        props.put("sasl.jaas.config", jaasConfig);
        props.put("sasl.login.callback.handler.class", 
                 "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler");
        
        // Additional consumer properties
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}

// src/main/java/com/example/kafka/service/KafkaConsumerService.java
package com.example.kafka.service;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final String LOG_FILE_PATH = "kafka-messages.log";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.consumer.group-id}")
    public void consume(@Payload Object message, 
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset) {
        
        try {
            logger.info("Received message from topic: {}, partition: {}, offset: {}", topic, partition, offset);
            
            if (message instanceof GenericRecord) {
                GenericRecord genericRecord = (GenericRecord) message;
                logger.info("Message converted to GenericRecord: {}", genericRecord);
                
                // Write to log file
                writeToLogFile(genericRecord, topic, partition, offset);
                
            } else {
                logger.warn("Message is not a GenericRecord: {}", message.getClass().getName());
                // Convert to GenericRecord if possible
                GenericRecord genericRecord = convertToGenericRecord(message);
                if (genericRecord != null) {
                    writeToLogFile(genericRecord, topic, partition, offset);
                }
            }
            
        } catch (Exception e) {
            logger.error("Error processing message: ", e);
        }
    }

    private GenericRecord convertToGenericRecord(Object message) {
        // This method would contain logic to convert the message to GenericRecord
        // The exact implementation depends on your message format
        logger.info("Attempting to convert message to GenericRecord: {}", message);
        
        // If the message is already a GenericRecord, return it
        if (message instanceof GenericRecord) {
            return (GenericRecord) message;
        }
        
        // Add custom conversion logic here based on your message format
        logger.warn("Unable to convert message to GenericRecord: {}", message.getClass().getName());
        return null;
    }

    private void writeToLogFile(GenericRecord record, String topic, int partition, long offset) {
        try (FileWriter writer = new FileWriter(LOG_FILE_PATH, true)) {
            String timestamp = LocalDateTime.now().format(formatter);
            String logEntry = String.format("[%s] Topic: %s, Partition: %d, Offset: %d, Record: %s%n",
                    timestamp, topic, partition, offset, record.toString());
            
            writer.write(logEntry);
            writer.flush();
            
            logger.info("Successfully wrote GenericRecord to log file");
            
        } catch (IOException e) {
            logger.error("Error writing to log file: ", e);
        }
    }
}

// src/main/java/com/example/kafka/service/MessageProcessor.java
package com.example.kafka.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class MessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    public GenericRecord processMessage(Object message) {
        if (message instanceof GenericRecord) {
            return (GenericRecord) message;
        }

        // Handle Map-based messages (common with JSON deserialization)
        if (message instanceof Map) {
            return convertMapToGenericRecord((Map<String, Object>) message);
        }

        // Handle String messages
        if (message instanceof String) {
            return convertStringToGenericRecord((String) message);
        }

        logger.warn("Unsupported message type: {}", message.getClass().getName());
        return null;
    }

    private GenericRecord convertMapToGenericRecord(Map<String, Object> messageMap) {
        try {
            // Create a simple schema for the map
            Schema.Builder schemaBuilder = Schema.builder().type(Schema.Type.RECORD).name("DynamicMessage");
            
            for (String key : messageMap.keySet()) {
                Object value = messageMap.get(key);
                Schema.Type fieldType = inferSchemaType(value);
                schemaBuilder.field(key).type().nullable().type(fieldType).noDefault();
            }
            
            Schema schema = schemaBuilder.endRecord();
            GenericRecord record = new GenericData.Record(schema);
            
            for (Map.Entry<String, Object> entry : messageMap.entrySet()) {
                record.put(entry.getKey(), entry.getValue());
            }
            
            return record;
            
        } catch (Exception e) {
            logger.error("Error converting Map to GenericRecord: ", e);
            return null;
        }
    }

    private GenericRecord convertStringToGenericRecord(String message) {
        try {
            // Create a simple schema for string message
            Schema schema = Schema.builder()
                .type(Schema.Type.RECORD)
                .name("StringMessage")
                .field("content").type().stringType().noDefault()
                .field("timestamp").type().longType().noDefault()
                .endRecord();
            
            GenericRecord record = new GenericData.Record(schema);
            record.put("content", message);
            record.put("timestamp", System.currentTimeMillis());
            
            return record;
            
        } catch (Exception e) {
            logger.error("Error converting String to GenericRecord: ", e);
            return null;
        }
    }

    private Schema.Type inferSchemaType(Object value) {
        if (value instanceof String) return Schema.Type.STRING;
        if (value instanceof Integer) return Schema.Type.INT;
        if (value instanceof Long) return Schema.Type.LONG;
        if (value instanceof Double) return Schema.Type.DOUBLE;
        if (value instanceof Float) return Schema.Type.FLOAT;
        if (value instanceof Boolean) return Schema.Type.BOOLEAN;
        return Schema.Type.STRING; // Default to string
    }
}

// src/main/resources/application.yml
server:
  port: 8080

spring:
  application:
    name: kafka-oauth-consumer

kafka:
  bootstrap-servers: your-kafka-broker:9092
  schema-registry:
    url: http://your-schema-registry:8081
  
  security:
    protocol: SASL_SSL
  
  sasl:
    mechanism: OAUTHBEARER
    jaas:
      config: ""  # This will be constructed programmatically
  
  consumer:
    group-id: oauth-consumer-group
    auto-offset-reset: earliest
    enable-auto-commit: true
  
  topic:
    name: your-topic-name
  
  oauth:
    client-id: ${KAFKA_OAUTH_CLIENT_ID:your-client-id}
    client-secret: ${KAFKA_OAUTH_CLIENT_SECRET:your-client-secret}
    token-endpoint: ${KAFKA_OAUTH_TOKEN_ENDPOINT:https://your-oauth-provider/oauth/token}

logging:
  level:
    com.example.kafka: DEBUG
    org.apache.kafka: INFO
    org.springframework.kafka: DEBUG
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} - %msg%n"
    file: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
  file:
    name: application.log

# src/main/resources/logback-spring.xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>application.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>application.%d{yyyy-MM-dd}.%i.log</fileNamePattern>
            <maxFileSize>10MB</maxFileSize>
            <maxHistory>30</maxHistory>
            <totalSizeCap>1GB</totalSizeCap>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="KAFKA_MESSAGES" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>kafka-messages.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>kafka-messages.%d{yyyy-MM-dd}.%i.log</fileNamePattern>
            <maxFileSize>50MB</maxFileSize>
            <maxHistory>7</maxHistory>
            <totalSizeCap>1GB</totalSizeCap>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss} - %msg%n</pattern>
        </encoder>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
        <appender-ref ref="FILE"/>
    </root>

    <logger name="com.example.kafka" level="DEBUG"/>
</configuration>
