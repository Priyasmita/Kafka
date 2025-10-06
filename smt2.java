package your.custom.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

public class RewriteNamespaceTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private String targetNamespace;

    @Override
    public void configure(Map<String, ?> configs) {
        targetNamespace = (String) configs.get("namespace");
    }

    @Override
    public R apply(R record) {
        Schema originalSchema = record.valueSchema();
        if (originalSchema == null || originalSchema.name() == null) {
            return record;
        }

        Schema newSchema = SchemaBuilder.struct()
            .name(targetNamespace + "." + originalSchema.name())
            .fields(originalSchema.fields())
            .build();

        Struct newValue = new Struct(newSchema);
        for (org.apache.kafka.connect.data.Field field : originalSchema.fields()) {
            newValue.put(field.name(), ((Struct) record.value()).get(field));
        }

        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            newSchema,
            newValue,
            record.timestamp()
        );
    }

    @Override
    public void close() {}

    @Override
    public ConfigDef config() {
        return new ConfigDef().define("namespace", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target Avro namespace");
    }
}
