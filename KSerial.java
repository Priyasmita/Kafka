import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.util.Map;

public class NamespaceStrippingAvroDeserializer<T extends SpecificRecord> extends KafkaAvroDeserializer {

    private final Class<T> targetClass;

    public NamespaceStrippingAvroDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            // Get the writer schema from the Confluent wire format
            Object genericRecord = super.deserialize(topic, bytes);
            Schema writerSchema = ((SpecificRecord) genericRecord).getSchema();

            // Load the reader schema from generated class (unified namespace)
            Schema readerSchema = targetClass.newInstance().getSchema();

            // Deserialize using the writer schema and reader schema
            SpecificDatumReader<T> reader = new SpecificDatumReader<>(writerSchema, readerSchema);
            return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));

        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Avro record", e);
        }
    }
}
