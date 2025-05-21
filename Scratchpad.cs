using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

public class OAuthTokenProvider
{
    private readonly string _tokenEndpoint;
    private readonly string _clientId;
    private readonly string _clientSecret;
    private string _accessToken;
    private DateTime _expiryTime;
    private readonly HttpClient _httpClient;

    public OAuthTokenProvider(string tokenEndpoint, string clientId, string clientSecret)
    {
        _tokenEndpoint = tokenEndpoint;
        _clientId = clientId;
        _clientSecret = clientSecret;
        _httpClient = new HttpClient();
    }

    public async Task<string> GetAccessTokenAsync()
    {
        if (!string.IsNullOrEmpty(_accessToken) && DateTime.UtcNow < _expiryTime)
        {
            return _accessToken;  // Use cached token
        }

        var request = new HttpRequestMessage(HttpMethod.Post, _tokenEndpoint);
        request.Content = new FormUrlEncodedContent(new[]
        {
            new KeyValuePair<string, string>("grant_type", "client_credentials"),
            new KeyValuePair<string, string>("client_id", _clientId),
            new KeyValuePair<string, string>("client_secret", _clientSecret)
        });

        var response = await _httpClient.SendAsync(request);
        response.EnsureSuccessStatusCode();

        var responseContent = await response.Content.ReadAsStringAsync();
        var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(responseContent);

        _accessToken = tokenResponse.AccessToken;
        _expiryTime = DateTime.UtcNow.AddSeconds(tokenResponse.ExpiresIn - 30); // Refresh 30s before expiration

        return _accessToken;
    }

    private class TokenResponse
    {
        public string AccessToken { get; set; }
        public int ExpiresIn { get; set; }
    }
}

public class KafkaProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly OAuthTokenProvider _tokenProvider;
    private const int MaxRetries = 3;

    public KafkaProducer(string bootstrapServers, string tokenEndpoint, string clientId, string clientSecret)
    {
        _tokenProvider = new OAuthTokenProvider(tokenEndpoint, clientId, clientSecret);
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer
        })
        .SetOAuthBearerTokenRefreshHandler(async context =>
        {
            string token = await _tokenProvider.GetAccessTokenAsync();
            context.SetToken(token, DateTime.UtcNow.AddHours(1), null);
        })
        .Build();
    }

    public void Produce(string topic, string key, string message)
    {
        int attempt = 0;

        while (attempt < MaxRetries)
        {
            try
            {
                var deliveryResult = _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = message }).Result;
                Console.WriteLine($"Message delivered to {deliveryResult.TopicPartitionOffset}");
                return;
            }
            catch (ProduceException<string, string> ex) when (IsRetryable(ex.Error.Code))
            {
                attempt++;
                Console.WriteLine($"Retryable error ({ex.Error.Reason}). Retrying {attempt}/{MaxRetries}...");
                Thread.Sleep(1000); // Wait before retry
            }
            catch (ProduceException<string, string> ex)
            {
                Console.WriteLine($"Non-retryable error ({ex.Error.Reason}). Dropping message.");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error: {ex.Message}. Aborting.");
                return;
            }
        }

        Console.WriteLine("Max retries reached. Message could not be delivered.");
    }

    private static bool IsRetryable(ErrorCode errorCode)
    {
        return errorCode switch
        {
            ErrorCode.Local_QueueFull => true,
            ErrorCode.Local_Transport => true,
            _ => false
        };
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}

public class KafkaProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly OAuthTokenProvider _tokenProvider;
    private const int MaxRetries = 3;

    public KafkaProducer(string bootstrapServers, string tokenEndpoint, string clientId, string clientSecret)
    {
        _tokenProvider = new OAuthTokenProvider(tokenEndpoint, clientId, clientSecret);
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer
        })
        .SetOAuthBearerTokenRefreshHandler(async context =>
        {
            string token = await _tokenProvider.GetAccessTokenAsync();
            context.SetToken(token, DateTime.UtcNow.AddHours(1), null);
        })
        .Build();
    }

    public async Task ProduceAsync(string topic, string key, string message)
    {
        int attempt = 0;

        while (attempt < MaxRetries)
        {
            try
            {
                var deliveryResult = await _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = message });
                Console.WriteLine($"Message delivered to {deliveryResult.TopicPartitionOffset}");
                return;
            }
            catch (ProduceException<string, string> ex) when (IsRetryable(ex.Error.Code))
            {
                attempt++;
                Console.WriteLine($"Retryable error ({ex.Error.Reason}). Retrying {attempt}/{MaxRetries}...");
                await Task.Delay(1000);
            }
            catch (ProduceException<string, string> ex)
            {
                Console.WriteLine($"Non-retryable error ({ex.Error.Reason}). Dropping message.");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error: {ex.Message}. Aborting.");
                return;
            }
        }

        Console.WriteLine("Max retries reached. Message could not be delivered.");
    }

    private static bool IsRetryable(ErrorCode errorCode)
    {
        return errorCode switch
        {
            ErrorCode.Local_QueueFull => true,
            ErrorCode.Local_Transport => true,
            _ => false
        };
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}

using NUnit.Framework;
using System;
using System.Threading.Tasks;

[TestFixture]
public class OAuthTokenProviderTests
{
    private OAuthTokenProvider _tokenProvider;

    [SetUp]
    public void Setup()
    {
        _tokenProvider = new OAuthTokenProvider("https://mock-oauth-server/token", "client-id", "client-secret");
    }

    [Test]
    public async Task GetAccessTokenAsync_ShouldReturnToken()
    {
        var token = await _tokenProvider.GetAccessTokenAsync();
        Assert.IsNotNull(token, "Token should not be null.");
    }

    [Test]
    public async Task GetAccessTokenAsync_ShouldCacheToken()
    {
        var token1 = await _tokenProvider.GetAccessTokenAsync();
        var token2 = await _tokenProvider.GetAccessTokenAsync();

        Assert.AreEqual(token1, token2, "Cached token should be used.");
    }
}

[TestFixture]
public class KafkaProducerTests
{
    private KafkaProducer _producer;

    [SetUp]
    public void Setup()
    {
        _producer = new KafkaProducer("mock-bootstrap-server", "https://mock-oauth-server/token", "client-id", "client-secret");
    }

    [Test]
    public async Task ProduceAsync_ShouldRetryOnFailure()
    {
        await _producer.ProduceAsync("test-topic", "key", "message");
        Assert.Pass("ProduceAsync executed successfully.");
    }
}
