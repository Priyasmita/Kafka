using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafk.Lib.Utils;
using Kafka.Lib.Exceptions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Kafka.Lib.Producer
{
    public class PlainKafkaProducer<TKey, TValue>(IConfiguration configuration, ILogger<PlainKafkaProducer<TKey, TValue>> logger) : BaseKafkaProducer<TKey, TValue>(
            configuration["Enterprise:Kafka:Topic"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:Topic]")
                , new ProducerBuilder<TKey, TValue>(new ProducerConfig
                {
                    BootstrapServers = configuration["Enterprise:Kafka:BootstrapServers"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:BootstrapServers]"),
                    ClientId = configuration["Enterprise:Kafka:ClientId"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:ClientId]"),
                    SaslUsername = configuration["Enterprise:Kafka:ApiKey"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:ApiKey]"),
                    SaslPassword = configuration["Enterprise:Kafka:ApiSecret"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:ApiSecret]"),

                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain,
                    Acks = Acks.All,
                    EnableIdempotence = true,
                    RequestTimeoutMs = 1000,
                    MessageSendMaxRetries = configuration["Enterprise:Kafka:MessageSendMaxRetries"] == null ? Constants.MAX_NUMBER_OF_MESSGE_RETRIES : Convert.ToInt32(configuration["Enterprise:Kafka:MessageSendMaxRetries"])
                })
                .SetValueSerializer(new AvroSerializer<TValue>(
                    new CachedSchemaRegistryClient(new SchemaRegistryConfig
                    {
                        Url = configuration["Enterprise:Kafka:SchemaRegistry:Url"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:SchemaRegistry.Url]"),
                        BasicAuthUserInfo = configuration["Enterprise:Kafka:SchemaRegistry:ApiKey"]
                        + ":"
                        + configuration["Enterprise:Kafka:SchemaRegistry:ApiSecret"]
                    }))).Build()
                , logger ?? throw new ArgumentNullException("Logger cannot be null"))
    {
    }
}
