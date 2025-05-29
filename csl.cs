using System;
using System.IO;
using System.Threading;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace KafkaAvroConsumer
{
    class Program
    {
        private static readonly string BootstrapServers = "your-kafka-broker:9092";
        private static readonly string SchemaRegistryUrl = "http://your-schema-registry:8081";
        private static readonly string TopicName = "your-avro-topic";
        private static readonly string ConsumerGroup = "avro-consumer-group";
        private static readonly string OutputFile = "response.txt";
        
        // OAuth Configuration
        private static readonly string OAuthTokenEndpoint = "https://your-oauth-provider/oauth/token";
        private static readonly string ClientId = "your-client-id";
        private static readonly string ClientSecret = "your-client-secret";
        private static readonly string Scope = "kafka:read";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Kafka AVRO Consumer with OAuth...");
            
            try
            {
                await ConsumeAvroMessages();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }

        private static async Task ConsumeAvroMessages()
        {
            // Get OAuth token
            string accessToken = await GetOAuthToken();
            
            // Configure Schema Registry client
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = SchemaRegistryUrl,
                // Add authentication if schema registry also requires OAuth
                // BasicAuthUserInfo = $"{ClientId}:{ClientSecret}"
            };

            // Configure Kafka consumer with OAuth
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = ConsumerGroup,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                
                // OAuth Configuration
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
                SaslOauthbearerTokenEndpointUrl = OAuthTokenEndpoint,
                SaslOauthbearerClientId = ClientId,
                SaslOauthbearerClientSecret = ClientSecret,
                SaslOauthbearerScope = Scope,
                
                // SSL Configuration (adjust as needed)
                SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https,
                SslCaLocation = "path/to/ca-cert.pem", // Optional: CA certificate path
            };

            using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
            using var consumer = new ConsumerBuilder<string, GenericRecord>(consumerConfig)
                .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry))
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .Build();

            consumer.Subscribe(TopicName);
            Console.WriteLine($"Subscribed to topic: {TopicName}");
            Console.WriteLine($"Output file: {OutputFile}");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
                Console.WriteLine("Cancelling...");
            };

            try
            {
                using var fileWriter = new StreamWriter(OutputFile, append: true);
                
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        
                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"Reached end of partition {consumeResult.Partition}");
                            continue;
                        }

                        // Convert AVRO GenericRecord to JSON
                        string jsonString = ConvertAvroToJson(consumeResult.Value);
                        
                        // Create a message wrapper with metadata
                        var messageWrapper = new
                        {
                            Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                            Topic = consumeResult.Topic,
                            Partition = consumeResult.Partition.Value,
                            Offset = consumeResult.Offset.Value,
                            Key = consumeResult.Message.Key,
                            Value = JsonConvert.DeserializeObject(jsonString)
                        };

                        string wrappedJson = JsonConvert.SerializeObject(messageWrapper, Formatting.Indented);
                        
                        // Write to file
                        await fileWriter.WriteLineAsync(wrappedJson);
                        await fileWriter.WriteLineAsync("---"); // Separator between messages
                        await fileWriter.FlushAsync();
                        
                        Console.WriteLine($"Processed message - Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}");
                        
                        // Commit the offset
                        consumer.Commit(consumeResult);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                        
                        // Handle OAuth token refresh if needed
                        if (e.Error.Code == ErrorCode.SaslAuthenticationFailed)
                        {
                            Console.WriteLine("Authentication failed. Attempting to refresh token...");
                            // In a production scenario, you might want to refresh the token here
                            await Task.Delay(5000); // Wait before retrying
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Unexpected error: {ex.Message}");
                        await Task.Delay(1000); // Brief pause before continuing
                    }
                }
            }
            finally
            {
                consumer.Close();
                Console.WriteLine("Consumer closed.");
            }
        }

        private static string ConvertAvroToJson(GenericRecord avroRecord)
        {
            // Convert AVRO GenericRecord to a dictionary for JSON serialization
            var dictionary = new Dictionary<string, object>();
            
            foreach (var field in avroRecord.Schema.Fields)
            {
                var fieldValue = avroRecord[field.Name];
                dictionary[field.Name] = ConvertAvroValue(fieldValue);
            }
            
            return JsonConvert.SerializeObject(dictionary, Formatting.Indented);
        }

        private static object ConvertAvroValue(object avroValue)
        {
            if (avroValue == null)
                return null;

            // Handle different AVRO types
            switch (avroValue)
            {
                case GenericRecord nestedRecord:
                    var nestedDict = new Dictionary<string, object>();
                    foreach (var field in nestedRecord.Schema.Fields)
                    {
                        nestedDict[field.Name] = ConvertAvroValue(nestedRecord[field.Name]);
                    }
                    return nestedDict;
                    
                case Array array:
                    var list = new List<object>();
                    foreach (var item in array)
                    {
                        list.Add(ConvertAvroValue(item));
                    }
                    return list;
                    
                case Dictionary<string, object> map:
                    var convertedMap = new Dictionary<string, object>();
                    foreach (var kvp in map)
                    {
                        convertedMap[kvp.Key] = ConvertAvroValue(kvp.Value);
                    }
                    return convertedMap;
                    
                case byte[] bytes:
                    return Convert.ToBase64String(bytes);
                    
                default:
                    return avroValue;
            }
        }

        private static async Task<string> GetOAuthToken()
        {
            // Simple OAuth2 client credentials flow implementation
            using var httpClient = new System.Net.Http.HttpClient();
            
            var tokenRequest = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("grant_type", "client_credentials"),
                new KeyValuePair<string, string>("client_id", ClientId),
                new KeyValuePair<string, string>("client_secret", ClientSecret),
                new KeyValuePair<string, string>("scope", Scope)
            };

            var requestContent = new System.Net.Http.FormUrlEncodedContent(tokenRequest);
            
            try
            {
                var response = await httpClient.PostAsync(OAuthTokenEndpoint, requestContent);
                var responseContent = await response.Content.ReadAsStringAsync();
                
                if (response.IsSuccessStatusCode)
                {
                    dynamic tokenResponse = JsonConvert.DeserializeObject(responseContent);
                    Console.WriteLine("OAuth token obtained successfully");
                    return tokenResponse.access_token;
                }
                else
                {
                    throw new Exception($"Failed to obtain OAuth token: {response.StatusCode} - {responseContent}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error obtaining OAuth token: {ex.Message}");
                throw;
            }
        }
    }
}
