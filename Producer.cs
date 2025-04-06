using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafk.Lib.Utils;
using Kafka.Lib.Exceptions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Kafka.Lib.Producer
{
    //TODO: Verify OAuth2 to Kafka and SchemaRegistry
    public class OauthKafkaProducer<TKey, TValue>(IConfiguration configuration, ILogger<OauthKafkaProducer<TKey, TValue>> logger) : BaseKafkaProducer<TKey, TValue>(
            configuration["Enterprise:Kafka:Topic"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:Topic]")
                , new ProducerBuilder<TKey, TValue>(new ProducerConfig
                {
                    BootstrapServers = configuration["Enterprise:Kafka:BootstrapServers"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:BootstrapServers]"),
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.OAuthBearer,
                    SaslOauthbearerClientId = configuration["Enterprise:Kafka:OAuth2ClientId"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:OAuth2ClientId]"),
                    SaslOauthbearerClientSecret = configuration["Enterprise:Kafka:OAuth2ClientSecret"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:OAuth2ClientSecret]"),
                    SaslOauthbearerTokenEndpointUrl = configuration["Enterprise:Kafka:OAuth2TokenProviderUrl"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:OAuth2TokenProviderUrl]"),
                    Acks = Acks.All,
                    EnableIdempotence = true,
                    MessageSendMaxRetries = configuration["Enterprise:Kafka:MessageSendMaxRetries"] == null ? Constants.MAX_NUMBER_OF_MESSGE_RETRIES : Convert.ToInt32(configuration["Enterprise:Kafka:MessageSendMaxRetries"])
                })
                .SetValueSerializer(new AvroSerializer<TValue>(
                    new CachedSchemaRegistryClient(new SchemaRegistryConfig
                    {
                        Url = configuration["Enterprise:Kafka:SchemaRegistry:Url"] ?? throw new RequiredKafkaConfigMissingException("Missing mandatory configuration element [Enterprise:Kafka:SchemaRegistry.Url]"),
                        BasicAuthUserInfo = configuration["Enterprise:Kafka:OAuth2ClientId"]
                        + ":"
                        + configuration["Enterprise:Kafka:SchemaRegistry:OAuth2ClientSecret"]
                    }))).Build()
                , logger ?? throw new ArgumentNullException("Logger cannot be null"))
    {
    }
}
