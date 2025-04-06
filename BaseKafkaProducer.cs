using Confluent.Kafka;
using Kafk.Lib.Utils;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text;

namespace Kafka.Lib.Producer
{
    public class BaseKafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
    {
        private readonly string _topic;
        private IProducer<TKey, TValue> _producer;
        private readonly ILogger<BaseKafkaProducer<TKey, TValue>> _logger;

        public BaseKafkaProducer(string topic, IProducer<TKey, TValue> producer, ILogger<BaseKafkaProducer<TKey, TValue>> logger)
        {
            ArgumentNullException.ThrowIfNull(logger);
            _topic = topic;
            _producer = producer;
            _logger = logger;
        }

        public async Task ProduceAsync(TKey key, TValue value, Headers headers)
        {
            try
            {
                var mandatoryHeader = headers.FirstOrDefault(hdr => hdr.Key == Constants.HDR_CORRELATION_ID);
                if (mandatoryHeader == null)
                {
                    throw new ArgumentNullException($"Mandatory header [{Constants.HDR_CORRELATION_ID}] is missing");
                }

                var message = new Message<TKey, TValue>
                {
                    Key = key,
                    Value = value,
                    Headers = new Headers()
                };

                foreach (var header in headers)
                {
                    message.Headers.Add(header.Key, header.GetValueBytes());
                }

                _logger.LogInformation($"[KAFKA_MESSAGE]: Header[{Constants.HDR_CORRELATION_ID}]={Encoding.UTF8.GetString(mandatoryHeader.GetValueBytes())}, Value={JsonConvert.SerializeObject(value, Formatting.None, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore })}");
                var deliveryResult = await _producer.ProduceAsync(_topic, message);
                _logger.LogInformation($"[KAFKA_RESPONSE] - Header[{Constants.HDR_CORRELATION_ID}]={Encoding.UTF8.GetString(mandatoryHeader.GetValueBytes())}, timestamp(unixMs)={deliveryResult.Timestamp.UnixTimestampMs}, partition={deliveryResult.TopicPartition.Partition}, offset={deliveryResult.TopicPartitionOffset.Offset}, status={deliveryResult.Status}");
            }
            catch (ProduceException<TKey, TValue> ex)
            {
                if (ex?.Error != null && IsRetryableError(ex.Error))
                {
                    _logger.LogWarning($"[PRODUCER_EXCEPTION_REASON] - {ex.Error.Reason}");
                }
                else
                {
                    _logger.LogError($"[PRODUCER_EXCEPTION_ERROR] - {ex?.Error.ToString()}");
                    _logger.LogError($"[PRODUCER_EXCEPTION_MESSAGE] - {ex?.InnerException?.Message}");
                    _logger.LogError($"[PRODUCER_EXCEPTION_STACKTRACE] - {ex?.StackTrace}");
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"[GENERIC_EXCEPTION_MESSAGE] - {ex.Message}");
                _logger.LogError($"[GENERIC_EXCEPTION_STACKTRACE] - {ex.StackTrace}");
                throw;
            }
        }

        public void Dispose()
        {
            _producer?.Dispose();
        }

        private bool IsRetryableError(Error error)
        {
            var ans= error.IsError &&
                   (error.Code == ErrorCode.NetworkException ||
                    error.Code == ErrorCode.BrokerNotAvailable ||
                    error.Code == ErrorCode.LeaderNotAvailable ||
                    error.Code == ErrorCode.RequestTimedOut ||
                    error.Code == ErrorCode.Local_TimedOut);
            _logger.LogError("[RETRYABLE_EXCEPTION]");
            return ans;
        }
    }
}
