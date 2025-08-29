// ========== Services/MessageProcessor.cs ==========
using System;
using System.Threading;
using TIBCO.EMS;
using TibcoEmsService.Configuration;

namespace TibcoEmsService.Services
{
    public class MessageProcessor : IMessageProcessor
    {
        private readonly IEmsConnectionFactory _connectionFactory;
        private readonly IJsonTransformer _jsonTransformer;
        private readonly IFileWriter _fileWriter;
        private readonly IErrorHandler _errorHandler;
        private readonly IServiceConfiguration _configuration;
        private IConnection _connection;
        private ISession _session;
        private IMessageConsumer _consumer;
        private IMessageProducer _errorProducer;
        private volatile bool _isProcessing;
        private volatile bool _isPaused;
        private volatile bool _stopRequested;
        private readonly object _processingLock = new object();
        private long _messageCount = 0;
        private long _successfulMessageCount = 0;
        private long _errorMessageCount = 0;
        private CancellationTokenSource _serviceCancellation;
        
        public MessageProcessor(
            IEmsConnectionFactory connectionFactory,
            IJsonTransformer jsonTransformer,
            IFileWriter fileWriter,
            IErrorHandler errorHandler,
            IServiceConfiguration configuration)
        {
            _connectionFactory = connectionFactory;
            _jsonTransformer = jsonTransformer;
            _fileWriter = fileWriter;
            _errorHandler = errorHandler;
            _configuration = configuration;
            _isPaused = false;
            _stopRequested = false;
            
            // Subscribe to Kafka failure events
            _fileWriter.OnCriticalFailure += HandleKafkaFailure;
        }
        
        public void SetServiceCancellation(CancellationTokenSource serviceCancellation)
        {
            _serviceCancellation = serviceCancellation;
        }
        
        private void HandleKafkaFailure(object sender, KafkaFailureEventArgs e)
        {
            _errorHandler.HandleError(
                $"CRITICAL KAFKA FAILURE: {e.ErrorMessage}. Shutting down service after {e.FailureCount} failures.", 
                new Exception("Kafka critical failure"));
            
            // Request immediate service shutdown
            RequestStop();
            
            // Signal service cancellation
            _serviceCancellation?.Cancel();
        }
        
        public void StartProcessing(CancellationToken cancellationToken)
        {
            try
            {
                InitializeConnections();
                
                while (!cancellationToken.IsCancellationRequested && !_stopRequested)
                {
                    try
                    {
                        // Check if processing is paused - but don't check while processing a message
                        if (_isPaused && !_isProcessing)
                        {
                            // Sleep briefly and continue loop to check cancellation
                            Thread.Sleep(100);
                            continue;
                        }
                        
                        // Don't receive new messages if pause or stop is requested
                        if (_isPaused || _stopRequested)
                        {
                            Thread.Sleep(100);
                            continue;
                        }
                        
                        // Use the factory's ReadMessage method to get a TextMessage
                        var textMessage = _connectionFactory.ReadMessage(_consumer, 1);
                        
                        if (textMessage != null)
                        {
                            // Use lock to ensure atomic processing
                            lock (_processingLock)
                            {
                                _isProcessing = true;
                                ProcessMessage(textMessage);
                                _isProcessing = false;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _errorHandler.HandleError("Message processing error", ex);
                        lock (_processingLock)
                        {
                            _isProcessing = false;
                        }
                    }
                }
                
                // Final wait for any current message to complete
                WaitForProcessingToComplete();
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Fatal processing error", ex);
                throw;
            }
        }
        
        private void InitializeConnections()
        {
            _connection = _connectionFactory.CreateConnection();
            _session = _connectionFactory.CreateSession(_connection);
            _consumer = _connectionFactory.CreateConsumer(_session, _configuration.InputQueueName);
            _errorProducer = _connectionFactory.CreateProducer(_session, _configuration.ErrorQueueName);
        }
        
        private void ProcessMessage(ITextMessage textMessage)
        {
            try
            {
                // Message is already a TextMessage thanks to ReadMessage method
                string originalText = textMessage.Text;
                
                // Transform to JSON
                string jsonContent = _jsonTransformer.TransformToJson(originalText);
                
                // Write to Kafka - this must complete even if stop/pause is requested
                // If this fails after retries, it will trigger service shutdown
                _fileWriter.WriteToFile(jsonContent);
                
                // Acknowledge message
                textMessage.Acknowledge();
                
                // Increment counters for successful processing
                Interlocked.Increment(ref _messageCount);
                Interlocked.Increment(ref _successfulMessageCount);
            }
            catch (Exception ex)
            {
                HandleFailedMessage(textMessage, ex);
            }
        }
        
        private void HandleFailedMessage(ITextMessage message, Exception ex)
        {
            try
            {
                // Send to error queue - this must complete even if stop/pause is requested
                var errorMessage = _session.CreateTextMessage();
                errorMessage.Text = message?.Text ?? "Unknown message content";
                errorMessage.SetStringProperty("ErrorReason", ex.Message);
                errorMessage.SetStringProperty("ErrorTime", DateTime.UtcNow.ToString("O"));
                errorMessage.SetStringProperty("DestinationFailure", "Kafka");
                errorMessage.SetStringProperty("OriginalMessageId", message?.MessageID ?? "Unknown");
                
                _errorProducer.Send(errorMessage);
                
                // Acknowledge the original message
                message.Acknowledge();
                
                // Increment total message count and error count (but not successful count)
                Interlocked.Increment(ref _messageCount);
                Interlocked.Increment(ref _errorMessageCount);
                
                _errorHandler.HandleError("Message sent to error queue", ex);
            }
            catch (Exception errorEx)
            {
                _errorHandler.HandleError("Failed to send message to error queue", errorEx);
            }
        }
        
        public long GetMessageCount()
        {
            return Interlocked.Read(ref _messageCount);
        }
        
        public long GetSuccessfulMessageCount()
        {
            return Interlocked.Read(ref _successfulMessageCount);
        }
        
        public long GetErrorMessageCount()
        {
            return Interlocked.Read(ref _errorMessageCount);
        }
        
        public void PauseProcessing()
        {
            _errorHandler.HandleError("Pause requested - waiting for current message to complete", 
                new Exception("Processing pause requested"));
            
            // Wait for current message to complete before pausing
            WaitForProcessingToComplete();
            
            _isPaused = true;
            _errorHandler.HandleError("Message processing paused", new Exception("Processing paused"));
        }
        
        public void ResumeProcessing()
        {
            _isPaused = false;
            _errorHandler.HandleError("Message processing resumed", new Exception("Processing resumed"));
        }
        
        public bool IsPaused()
        {
            return _isPaused;
        }
        
        public void RequestStop()
        {
            _errorHandler.HandleError("Stop requested - waiting for current message to complete", 
                new Exception("Processing stop requested"));
            
            // Wait for current message to complete before stopping
            WaitForProcessingToComplete();
            
            _stopRequested = true;
            _errorHandler.HandleError("Message processing stopped", new Exception("Processing stopped"));
        }
        
        private void WaitForProcessingToComplete()
        {
            // Wait for current message to complete with timeout
            int waitCount = 0;
            while (_isProcessing && waitCount < 300) // Max 30 seconds (300 * 100ms)
            {
                Thread.Sleep(100);
                waitCount++;
            }
            
            if (_isProcessing)
            {
                _errorHandler.HandleError("Warning: Message processing did not complete within 30 seconds", 
                    new Exception("Processing timeout"));
            }
        }
        
        public void Dispose()
        {
            // Unsubscribe from Kafka failure events
            _fileWriter.OnCriticalFailure -= HandleKafkaFailure;
            
            // Ensure no message is being processed
            WaitForProcessingToComplete();
            
            _consumer?.Close();
            _errorProducer?.Close();
            _session?.Close();
            _connection?.Close();
            
            // Dispose the file writer (Kafka producer)
            _fileWriter?.Dispose();
        }
    }
}// ========== Program.cs ==========
using System;
using System.ServiceProcess;
using Autofac;
using TibcoEmsService.Services;
using TibcoEmsService.Configuration;

namespace TibcoEmsService
{
    static class Program
    {
        static void Main(string[] args)
        {
            var container = ConfigureContainer(args);
            
            using (var scope = container.BeginLifetimeScope())
            {
                var service = scope.Resolve<EmsProcessorService>();
                ServiceBase.Run(service);
            }
        }
        
        private static IContainer ConfigureContainer(string[] args)
        {
            var builder = new ContainerBuilder();
            
            // Register configuration with constructor parameter
            builder.RegisterType<ServiceConfiguration>()
                   .As<IServiceConfiguration>()
                   .WithParameter(new TypedParameter(typeof(string[]), args))
                   .SingleInstance();
            
            // Register services
            builder.RegisterType<EmsConnectionFactory>()
                   .As<IEmsConnectionFactory>()
                   .SingleInstance();
            
            builder.RegisterType<MessageProcessor>()
                   .As<IMessageProcessor>()
                   .InstancePerLifetimeScope();
            
            builder.RegisterType<JsonTransformer>()
                   .As<IJsonTransformer>()
                   .InstancePerLifetimeScope();
            
            // Register KafkaWriter for Kafka output
            builder.RegisterType<KafkaWriter>()
                   .As<IFileWriter>()
                   .InstancePerLifetimeScope();
            
            builder.RegisterType<HeartbeatService>()
                   .As<IHeartbeatService>()
                   .SingleInstance();
            
            builder.RegisterType<CommandService>()
                   .As<ICommandService>()
                   .SingleInstance();
            
            builder.RegisterType<ErrorHandler>()
                   .As<IErrorHandler>()
                   .InstancePerLifetimeScope();
            
            builder.RegisterType<EmsProcessorService>()
                   .AsSelf();
            
            return builder.Build();
        }
    }
}

// ========== Configuration/IServiceConfiguration.cs ==========
namespace TibcoEmsService.Configuration
{
    public interface IServiceConfiguration
    {
        string EmsServerUrl { get; }
        string Username { get; }
        string Password { get; }
        string InputQueueName { get; }
        string ErrorQueueName { get; }
        string HeartbeatQueueName { get; }
        string CommandTopicName { get; }
        string OutputDirectory { get; }
        string AutoMode { get; }
        int HeartbeatIntervalSeconds { get; }
        
        // Kafka Configuration
        string KafkaBootstrapServers { get; }
        string KafkaTopicName { get; }
        string KafkaClientId { get; }
        string KafkaSecurityProtocol { get; }
        string KafkaSaslMechanism { get; }
        string KafkaSaslUsername { get; }
        string KafkaSaslPassword { get; }
        int KafkaRetryCount { get; }
        int KafkaRetryDelayMs { get; }
    }
}

// ========== Configuration/ServiceConfiguration.cs ==========
using System;
using System.Configuration;

namespace TibcoEmsService.Configuration
{
    public class ServiceConfiguration : IServiceConfiguration
    {
        public string EmsServerUrl { get; private set; }
        public string Username { get; private set; }
        public string Password { get; private set; }
        public string InputQueueName { get; private set; }
        public string ErrorQueueName { get; private set; }
        public string HeartbeatQueueName { get; private set; }
        public string CommandTopicName { get; private set; }
        public string OutputDirectory { get; private set; }
        public string AutoMode { get; private set; }
        public int HeartbeatIntervalSeconds { get; private set; }
        
        // Kafka Configuration
        public string KafkaBootstrapServers { get; private set; }
        public string KafkaTopicName { get; private set; }
        public string KafkaClientId { get; private set; }
        public string KafkaSecurityProtocol { get; private set; }
        public string KafkaSaslMechanism { get; private set; }
        public string KafkaSaslUsername { get; private set; }
        public string KafkaSaslPassword { get; private set; }
        public int KafkaRetryCount { get; private set; }
        public int KafkaRetryDelayMs { get; private set; }
        
        public ServiceConfiguration(string[] args)
        {
            Initialize(args ?? new string[0]);
        }
        
        private void Initialize(string[] args)
        {
            EmsServerUrl = ConfigurationManager.AppSettings["EmsServerUrl"] ?? "tcp://localhost:7222";
            Username = ConfigurationManager.AppSettings["Username"] ?? "admin";
            Password = ConfigurationManager.AppSettings["Password"] ?? "admin";
            InputQueueName = ConfigurationManager.AppSettings["InputQueueName"] ?? "input.queue";
            ErrorQueueName = ConfigurationManager.AppSettings["ErrorQueueName"] ?? "error.queue";
            HeartbeatQueueName = ConfigurationManager.AppSettings["HeartbeatQueueName"] ?? "heartbeat.queue";
            CommandTopicName = ConfigurationManager.AppSettings["CommandTopicName"] ?? "command.topic";
            OutputDirectory = ConfigurationManager.AppSettings["OutputDirectory"] ?? @"C:\EmsOutput";
            AutoMode = args.Length > 0 && args[0].ToLower() == "y" ? "y" : "n";
            
            // Parse heartbeat interval with default of 60 seconds
            string heartbeatInterval = ConfigurationManager.AppSettings["HeartbeatIntervalSeconds"] ?? "60";
            if (!int.TryParse(heartbeatInterval, out int interval) || interval <= 0)
            {
                interval = 60; // Default to 60 seconds if invalid
            }
            HeartbeatIntervalSeconds = interval;
            
            // Kafka Configuration
            KafkaBootstrapServers = ConfigurationManager.AppSettings["KafkaBootstrapServers"] ?? "localhost:9092";
            KafkaTopicName = ConfigurationManager.AppSettings["KafkaTopicName"] ?? "ems-output-topic";
            KafkaClientId = ConfigurationManager.AppSettings["KafkaClientId"] ?? $"TibcoEmsProcessor_{Environment.MachineName}";
            KafkaSecurityProtocol = ConfigurationManager.AppSettings["KafkaSecurityProtocol"] ?? "Plaintext";
            KafkaSaslMechanism = ConfigurationManager.AppSettings["KafkaSaslMechanism"] ?? "";
            KafkaSaslUsername = ConfigurationManager.AppSettings["KafkaSaslUsername"] ?? "";
            KafkaSaslPassword = ConfigurationManager.AppSettings["KafkaSaslPassword"] ?? "";
            
            // Parse retry settings
            string retryCount = ConfigurationManager.AppSettings["KafkaRetryCount"] ?? "5";
            if (!int.TryParse(retryCount, out int retry) || retry <= 0)
            {
                retry = 5;
            }
            KafkaRetryCount = retry;
            
            string retryDelay = ConfigurationManager.AppSettings["KafkaRetryDelayMs"] ?? "2000";
            if (!int.TryParse(retryDelay, out int delay) || delay <= 0)
            {
                delay = 2000;
            }
            KafkaRetryDelayMs = delay;
        }
    }
}

// ========== Services/EmsProcessorService.cs ==========
using System;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace TibcoEmsService.Services
{
    public class EmsProcessorService : ServiceBase
    {
        private readonly IMessageProcessor _messageProcessor;
        private readonly IHeartbeatService _heartbeatService;
        private readonly ICommandService _commandService;
        private readonly IErrorHandler _errorHandler;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _processingTask;
        private Task _heartbeatTask;
        private Task _commandTask;
        
        public EmsProcessorService(
            IMessageProcessor messageProcessor,
            IHeartbeatService heartbeatService,
            ICommandService commandService,
            IErrorHandler errorHandler)
        {
            _messageProcessor = messageProcessor;
            _heartbeatService = heartbeatService;
            _commandService = commandService;
            _errorHandler = errorHandler;
            ServiceName = "TibcoEmsProcessorService";
        }
        
        protected override void OnStart(string[] args)
        {
            StartService(args);
        }
        
        public void StartService(string[] args)
        {
            try
            {
                _cancellationTokenSource = new CancellationTokenSource();
                
                // Pass cancellation source to message processor for Kafka failure handling
                if (_messageProcessor is MessageProcessor processor)
                {
                    processor.SetServiceCancellation(_cancellationTokenSource);
                }
                
                // Start message processing
                _processingTask = Task.Run(() => 
                    _messageProcessor.StartProcessing(_cancellationTokenSource.Token));
                
                // Start heartbeat service with reference to message processor for count
                _heartbeatTask = Task.Run(() => 
                    _heartbeatService.StartHeartbeat(_cancellationTokenSource.Token, _messageProcessor));
                
                // Start command service
                _commandTask = Task.Run(() =>
                    _commandService.StartCommandListener(
                        _cancellationTokenSource.Token,
                        _cancellationTokenSource,
                        _messageProcessor));
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Service startup failed", ex);
                throw;
            }
        }
        
        protected override void OnStop()
        {
            StopService();
        }
        
        public void StopService()
        {
            try
            {
                // Signal cancellation
                _cancellationTokenSource?.Cancel();
                
                // Wait for graceful shutdown
                Task.WaitAll(new[] { _processingTask, _heartbeatTask, _commandTask }, TimeSpan.FromSeconds(30));
                
                // Log final message counts
                var totalCount = _messageProcessor?.GetMessageCount() ?? 0;
                var successfulCount = _messageProcessor?.GetSuccessfulMessageCount() ?? 0;
                var errorCount = totalCount - successfulCount;
                
                _errorHandler.HandleError($"Service stopping. Total: {totalCount}, Successful: {successfulCount}, Errors: {errorCount}", 
                    new Exception("Service shutdown"));
                
                // Cleanup
                _messageProcessor?.Dispose();
                _heartbeatService?.Dispose();
                _commandService?.Dispose();
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Service shutdown error", ex);
            }
        }
    }
}

// ========== Services/IEmsConnectionFactory.cs ==========
using TIBCO.EMS;

namespace TibcoEmsService.Services
{
    public interface IEmsConnectionFactory
    {
        IConnection CreateConnection();
        ISession CreateSession(IConnection connection);
        IMessageConsumer CreateConsumer(ISession session, string queueName);
        IMessageProducer CreateProducer(ISession session, string queueName);
        IMessageConsumer CreateTopicSubscriber(ISession session, string topicName, string subscriptionName);
        ITextMessage ReadMessage(IMessageConsumer consumer, int timeoutSeconds);
    }
}

// ========== Services/EmsConnectionFactory.cs ==========
using System;
using TIBCO.EMS;
using TibcoEmsService.Configuration;

namespace TibcoEmsService.Services
{
    public class EmsConnectionFactory : IEmsConnectionFactory
    {
        private readonly IServiceConfiguration _configuration;
        private ConnectionFactory _connectionFactory;
        
        public EmsConnectionFactory(IServiceConfiguration configuration)
        {
            _configuration = configuration;
            InitializeFactory();
        }
        
        private void InitializeFactory()
        {
            _connectionFactory = new ConnectionFactory(_configuration.EmsServerUrl);
        }
        
        public IConnection CreateConnection()
        {
            var connection = _connectionFactory.CreateConnection(
                _configuration.Username, 
                _configuration.Password);
            connection.Start();
            return connection;
        }
        
        public ISession CreateSession(IConnection connection)
        {
            return connection.CreateSession(false, Session.CLIENT_ACKNOWLEDGE);
        }
        
        public IMessageConsumer CreateConsumer(ISession session, string queueName)
        {
            var queue = session.CreateQueue(queueName);
            return session.CreateConsumer(queue);
        }
        
        public IMessageProducer CreateProducer(ISession session, string queueName)
        {
            var queue = session.CreateQueue(queueName);
            return session.CreateProducer(queue);
        }
        
        public IMessageConsumer CreateTopicSubscriber(ISession session, string topicName, string subscriptionName)
        {
            var topic = session.CreateTopic(topicName);
            
            // Create a durable subscriber for the topic
            // Use subscription name to make it durable so messages aren't lost when service is down
            if (!string.IsNullOrEmpty(subscriptionName))
            {
                return session.CreateDurableSubscriber(topic, subscriptionName);
            }
            else
            {
                // Non-durable subscriber - messages are lost if service is not running
                return session.CreateConsumer(topic);
            }
        }
        
        public ITextMessage ReadMessage(IMessageConsumer consumer, int timeoutSeconds)
        {
            if (consumer == null)
            {
                throw new ArgumentNullException(nameof(consumer), "Message consumer cannot be null");
            }
            
            // Receive message with timeout
            var message = consumer.Receive(TimeSpan.FromSeconds(timeoutSeconds));
            
            if (message == null)
            {
                // No message received within timeout
                return null;
            }
            
            // Try to cast to TextMessage
            var textMessage = message as ITextMessage;
            
            if (textMessage != null)
            {
                // Already a TextMessage, return as-is
                return textMessage;
            }
            
            // Handle other message types
            if (message is IBytesMessage bytesMessage)
            {
                // Convert BytesMessage to TextMessage
                return ConvertBytesMessageToText(bytesMessage);
            }
            
            if (message is IMapMessage mapMessage)
            {
                // Convert MapMessage to TextMessage (as JSON)
                return ConvertMapMessageToText(mapMessage);
            }
            
            if (message is IObjectMessage objectMessage)
            {
                // Convert ObjectMessage to TextMessage
                return ConvertObjectMessageToText(objectMessage);
            }
            
            if (message is IStreamMessage streamMessage)
            {
                // Convert StreamMessage to TextMessage
                return ConvertStreamMessageToText(streamMessage);
            }
            
            // For any other message type, create a text message with basic info
            return CreateDefaultTextMessage(message);
        }
        
        private ITextMessage ConvertBytesMessageToText(IBytesMessage bytesMessage)
        {
            try
            {
                // Read bytes from the message
                byte[] bytes = new byte[bytesMessage.BodyLength];
                bytesMessage.ReadBytes(bytes);
                
                // Convert bytes to string (assuming UTF-8 encoding)
                string text = System.Text.Encoding.UTF8.GetString(bytes);
                
                // Create a new TextMessage with the converted content
                // Note: In real implementation, you'd need the session to create a new message
                // For now, we'll create a simple wrapper
                return new TextMessageWrapper(text, bytesMessage);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to convert BytesMessage to TextMessage: {ex.Message}", ex);
            }
        }
        
        private ITextMessage ConvertMapMessageToText(IMapMessage mapMessage)
        {
            try
            {
                // Convert MapMessage to JSON representation
                var jsonObject = new System.Collections.Generic.Dictionary<string, object>();
                var mapNames = mapMessage.GetMapNames();
                
                while (mapNames.MoveNext())
                {
                    string name = mapNames.Current as string;
                    if (name != null)
                    {
                        jsonObject[name] = mapMessage.GetObject(name);
                    }
                }
                
                string jsonText = Newtonsoft.Json.JsonConvert.SerializeObject(jsonObject);
                return new TextMessageWrapper(jsonText, mapMessage);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to convert MapMessage to TextMessage: {ex.Message}", ex);
            }
        }
        
        private ITextMessage ConvertObjectMessageToText(IObjectMessage objectMessage)
        {
            try
            {
                // Serialize the object to JSON
                var obj = objectMessage.TheObject;
                string jsonText = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
                return new TextMessageWrapper(jsonText, objectMessage);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to convert ObjectMessage to TextMessage: {ex.Message}", ex);
            }
        }
        
        private ITextMessage ConvertStreamMessageToText(IStreamMessage streamMessage)
        {
            try
            {
                // Read all values from the stream
                var values = new System.Collections.Generic.List<object>();
                streamMessage.Reset(); // Reset to beginning of stream
                
                try
                {
                    while (true)
                    {
                        values.Add(streamMessage.ReadObject());
                    }
                }
                catch (MessageEOFException)
                {
                    // End of stream reached
                }
                
                string jsonText = Newtonsoft.Json.JsonConvert.SerializeObject(values);
                return new TextMessageWrapper(jsonText, streamMessage);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to convert StreamMessage to TextMessage: {ex.Message}", ex);
            }
        }
        
        private ITextMessage CreateDefaultTextMessage(Message message)
        {
            // Create a text representation of the message metadata
            var messageInfo = new
            {
                messageId = message.MessageID,
                timestamp = message.Timestamp,
                correlationId = message.CorrelationID,
                type = message.GetType().Name,
                deliveryMode = message.DeliveryMode,
                priority = message.Priority
            };
            
            string jsonText = Newtonsoft.Json.JsonConvert.SerializeObject(messageInfo);
            return new TextMessageWrapper(jsonText, message);
        }
    }
    
    // Internal wrapper class to convert non-text messages to TextMessage interface
    internal class TextMessageWrapper : ITextMessage
    {
        private readonly string _text;
        private readonly Message _originalMessage;
        
        public TextMessageWrapper(string text, Message originalMessage)
        {
            _text = text;
            _originalMessage = originalMessage;
        }
        
        public string Text
        {
            get { return _text; }
            set { throw new NotSupportedException("Cannot modify wrapped message text"); }
        }
        
        // Delegate all other IMessage properties/methods to the original message
        public void Acknowledge() => _originalMessage.Acknowledge();
        public void ClearBody() => _originalMessage.ClearBody();
        public void ClearProperties() => _originalMessage.ClearProperties();
        
        public string MessageID
        {
            get => _originalMessage.MessageID;
            set => _originalMessage.MessageID = value;
        }
        
        public long Timestamp
        {
            get => _originalMessage.Timestamp;
            set => _originalMessage.Timestamp = value;
        }
        
        public string CorrelationID
        {
            get => _originalMessage.CorrelationID;
            set => _originalMessage.CorrelationID = value;
        }
        
        public Destination ReplyTo
        {
            get => _originalMessage.ReplyTo;
            set => _originalMessage.ReplyTo = value;
        }
        
        public Destination Destination
        {
            get => _originalMessage.Destination;
            set => _originalMessage.Destination = value;
        }
        
        public int DeliveryMode
        {
            get => _originalMessage.DeliveryMode;
            set => _originalMessage.DeliveryMode = value;
        }
        
        public bool Redelivered
        {
            get => _originalMessage.Redelivered;
            set => _originalMessage.Redelivered = value;
        }
        
        public string Type
        {
            get => _originalMessage.Type;
            set => _originalMessage.Type = value;
        }
        
        public long Expiration
        {
            get => _originalMessage.Expiration;
            set => _originalMessage.Expiration = value;
        }
        
        public int Priority
        {
            get => _originalMessage.Priority;
            set => _originalMessage.Priority = value;
        }
        
        public bool GetBooleanProperty(string name) => _originalMessage.GetBooleanProperty(name);
        public byte GetByteProperty(string name) => _originalMessage.GetByteProperty(name);
        public short GetShortProperty(string name) => _originalMessage.GetShortProperty(name);
        public int GetIntProperty(string name) => _originalMessage.GetIntProperty(name);
        public long GetLongProperty(string name) => _originalMessage.GetLongProperty(name);
        public float GetFloatProperty(string name) => _originalMessage.GetFloatProperty(name);
        public double GetDoubleProperty(string name) => _originalMessage.GetDoubleProperty(name);
        public string GetStringProperty(string name) => _originalMessage.GetStringProperty(name);
        public object GetObjectProperty(string name) => _originalMessage.GetObjectProperty(name);
        public System.Collections.IEnumerator GetPropertyNames() => _originalMessage.GetPropertyNames();
        public bool PropertyExists(string name) => _originalMessage.PropertyExists(name);
        
        public void SetBooleanProperty(string name, bool value) => _originalMessage.SetBooleanProperty(name, value);
        public void SetByteProperty(string name, byte value) => _originalMessage.SetByteProperty(name, value);
        public void SetShortProperty(string name, short value) => _originalMessage.SetShortProperty(name, value);
        public void SetIntProperty(string name, int value) => _originalMessage.SetIntProperty(name, value);
        public void SetLongProperty(string name, long value) => _originalMessage.SetLongProperty(name, value);
        public void SetFloatProperty(string name, float value) => _originalMessage.SetFloatProperty(name, value);
        public void SetDoubleProperty(string name, double value) => _originalMessage.SetDoubleProperty(name, value);
        public void SetStringProperty(string name, string value) => _originalMessage.SetStringProperty(name, value);
        public void SetObjectProperty(string name, object value) => _originalMessage.SetObjectProperty(name, value);
    }
}

// ========== Services/IMessageProcessor.cs ==========
using System;
using System.Threading;

namespace TibcoEmsService.Services
{
    public interface IMessageProcessor : IDisposable
    {
        void StartProcessing(CancellationToken cancellationToken);
        long GetMessageCount();
        long GetSuccessfulMessageCount();
    }
}

// ========== Services/MessageProcessor.cs ==========
using System;
using System.Threading;
using TIBCO.EMS;
using TibcoEmsService.Configuration;

namespace TibcoEmsService.Services
{
    public class MessageProcessor : IMessageProcessor
    {
        private readonly IEmsConnectionFactory _connectionFactory;
        private readonly IJsonTransformer _jsonTransformer;
        private readonly IFileWriter _fileWriter;
        private readonly IErrorHandler _errorHandler;
        private readonly IServiceConfiguration _configuration;
        private IConnection _connection;
        private ISession _session;
        private IMessageConsumer _consumer;
        private IMessageProducer _errorProducer;
        private volatile bool _isProcessing;
        private volatile bool _isPaused;
        private volatile bool _stopRequested;
        private readonly object _processingLock = new object();
        private long _messageCount = 0;
        private long _successfulMessageCount = 0;
        private long _errorMessageCount = 0;
        
        public MessageProcessor(
            IEmsConnectionFactory connectionFactory,
            IJsonTransformer jsonTransformer,
            IFileWriter fileWriter,
            IErrorHandler errorHandler,
            IServiceConfiguration configuration)
        {
            _connectionFactory = connectionFactory;
            _jsonTransformer = jsonTransformer;
            _fileWriter = fileWriter;
            _errorHandler = errorHandler;
            _configuration = configuration;
            _isPaused = false;
            _stopRequested = false;
        }
        
        public void StartProcessing(CancellationToken cancellationToken)
        {
            try
            {
                InitializeConnections();
                
                while (!cancellationToken.IsCancellationRequested && !_stopRequested)
                {
                    try
                    {
                        // Check if processing is paused - but don't check while processing a message
                        if (_isPaused && !_isProcessing)
                        {
                            // Sleep briefly and continue loop to check cancellation
                            Thread.Sleep(100);
                            continue;
                        }
                        
                        // Don't receive new messages if pause or stop is requested
                        if (_isPaused || _stopRequested)
                        {
                            Thread.Sleep(100);
                            continue;
                        }
                        
                        var message = _consumer.Receive(TimeSpan.FromSeconds(1));
                        
                        if (message != null)
                        {
                            // Use lock to ensure atomic processing
                            lock (_processingLock)
                            {
                                _isProcessing = true;
                                ProcessMessage(message);
                                _isProcessing = false;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _errorHandler.HandleError("Message processing error", ex);
                        lock (_processingLock)
                        {
                            _isProcessing = false;
                        }
                    }
                }
                
                // Final wait for any current message to complete
                WaitForProcessingToComplete();
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Fatal processing error", ex);
                throw;
            }
        }
        
        private void InitializeConnections()
        {
            _connection = _connectionFactory.CreateConnection();
            _session = _connectionFactory.CreateSession(_connection);
            _consumer = _connectionFactory.CreateConsumer(_session, _configuration.InputQueueName);
            _errorProducer = _connectionFactory.CreateProducer(_session, _configuration.ErrorQueueName);
        }
        
        private void ProcessMessage(Message message)
        {
            try
            {
                // Extract text from message
                var textMessage = message as TextMessage;
                if (textMessage == null)
                {
                    throw new InvalidOperationException("Message is not a TextMessage");
                }
                
                string originalText = textMessage.Text;
                
                // Transform to JSON
                string jsonContent = _jsonTransformer.TransformToJson(originalText);
                
                // Write to MQ - this must complete even if stop/pause is requested
                _fileWriter.WriteToFile(jsonContent);
                
                // Acknowledge message
                message.Acknowledge();
                
                // Increment counters for successful processing
                Interlocked.Increment(ref _messageCount);
                Interlocked.Increment(ref _successfulMessageCount);
            }
            catch (Exception ex)
            {
                HandleFailedMessage(message, ex);
            }
        }
        
        private void HandleFailedMessage(Message message, Exception ex)
        {
            try
            {
                // Send to error queue - this must complete even if stop/pause is requested
                var errorMessage = _session.CreateTextMessage();
                errorMessage.Text = (message as TextMessage)?.Text ?? "Unknown message content";
                errorMessage.SetStringProperty("ErrorReason", ex.Message);
                errorMessage.SetStringProperty("ErrorTime", DateTime.UtcNow.ToString("O"));
                
                _errorProducer.Send(errorMessage);
                
                // Acknowledge the original message
                message.Acknowledge();
                
                // Increment total message count and error count (but not successful count)
                Interlocked.Increment(ref _messageCount);
                Interlocked.Increment(ref _errorMessageCount);
                
                _errorHandler.HandleError("Message sent to error queue", ex);
            }
            catch (Exception errorEx)
            {
                _errorHandler.HandleError("Failed to send message to error queue", errorEx);
            }
        }
        
        public long GetMessageCount()
        {
            return Interlocked.Read(ref _messageCount);
        }
        
        public long GetSuccessfulMessageCount()
        {
            return Interlocked.Read(ref _successfulMessageCount);
        }
        
        public long GetErrorMessageCount()
        {
            return Interlocked.Read(ref _errorMessageCount);
        }
        
        public void PauseProcessing()
        {
            _errorHandler.HandleError("Pause requested - waiting for current message to complete", 
                new Exception("Processing pause requested"));
            
            // Wait for current message to complete before pausing
            WaitForProcessingToComplete();
            
            _isPaused = true;
            _errorHandler.HandleError("Message processing paused", new Exception("Processing paused"));
        }
        
        public void ResumeProcessing()
        {
            _isPaused = false;
            _errorHandler.HandleError("Message processing resumed", new Exception("Processing resumed"));
        }
        
        public bool IsPaused()
        {
            return _isPaused;
        }
        
        public void RequestStop()
        {
            _errorHandler.HandleError("Stop requested - waiting for current message to complete", 
                new Exception("Processing stop requested"));
            
            // Wait for current message to complete before stopping
            WaitForProcessingToComplete();
            
            _stopRequested = true;
            _errorHandler.HandleError("Message processing stopped", new Exception("Processing stopped"));
        }
        
        private void WaitForProcessingToComplete()
        {
            // Wait for current message to complete with timeout
            int waitCount = 0;
            while (_isProcessing && waitCount < 300) // Max 30 seconds (300 * 100ms)
            {
                Thread.Sleep(100);
                waitCount++;
            }
            
            if (_isProcessing)
            {
                _errorHandler.HandleError("Warning: Message processing did not complete within 30 seconds", 
                    new Exception("Processing timeout"));
            }
        }
        
        public void Dispose()
        {
            // Ensure no message is being processed
            WaitForProcessingToComplete();
            
            _consumer?.Close();
            _errorProducer?.Close();
            _session?.Close();
            _connection?.Close();
        }
    }
}

// ========== Services/IJsonTransformer.cs ==========
namespace TibcoEmsService.Services
{
    public interface IJsonTransformer
    {
        string TransformToJson(string input);
    }
}

// ========== Services/JsonTransformer.cs ==========
using System;
using Newtonsoft.Json;

namespace TibcoEmsService.Services
{
    public class JsonTransformer : IJsonTransformer
    {
        public string TransformToJson(string input)
        {
            try
            {
                // Try to parse as JSON first (already JSON)
                var test = JsonConvert.DeserializeObject(input);
                if (test != null)
                {
                    return input;
                }
            }
            catch
            {
                // Not JSON, continue with transformation
            }
            
            // Transform plain text to JSON
            var jsonObject = new
            {
                timestamp = DateTime.UtcNow.ToString("O"),
                messageType = "text",
                content = input,
                processedAt = DateTime.UtcNow.ToString("O")
            };
            
            return JsonConvert.SerializeObject(jsonObject, Formatting.Indented);
        }
    }
}

// ========== Services/IFileWriter.cs ==========
using System;

namespace TibcoEmsService.Services
{
    public interface IFileWriter : IDisposable
    {
        void WriteToFile(string content);
        event EventHandler<KafkaFailureEventArgs> OnCriticalFailure;
    }
    
    public class KafkaFailureEventArgs : EventArgs
    {
        public string ErrorMessage { get; set; }
        public int FailureCount { get; set; }
    }
}

// ========== Services/KafkaWriter.cs ==========
using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using TibcoEmsService.Configuration;

namespace TibcoEmsService.Services
{
    public class KafkaWriter : IFileWriter
    {
        private readonly IServiceConfiguration _configuration;
        private readonly IErrorHandler _errorHandler;
        private readonly object _lockObject = new object();
        private IProducer<string, string> _producer;
        private volatile bool _isConnected = false;
        private int _consecutiveFailures = 0;
        
        public event EventHandler<KafkaFailureEventArgs> OnCriticalFailure;
        
        public KafkaWriter(IServiceConfiguration configuration, IErrorHandler errorHandler)
        {
            _configuration = configuration;
            _errorHandler = errorHandler;
            InitializeProducer();
        }
        
        private void InitializeProducer()
        {
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = _configuration.KafkaBootstrapServers,
                    ClientId = _configuration.KafkaClientId,
                    
                    // Reliability settings
                    Acks = Acks.All,  // Wait for all replicas to acknowledge
                    EnableIdempotence = true,  // Ensure exactly-once delivery
                    MaxInFlight = 5,
                    MessageSendMaxRetries = 3,
                    RetryBackoffMs = 100,
                    
                    // Performance settings
                    LingerMs = 10,  // Small batching for better throughput
                    CompressionType = CompressionType.Snappy,
                    BatchSize = 16384,
                    
                    // Connection settings
                    SocketTimeoutMs = 30000,
                    RequestTimeoutMs = 30000,
                    MessageTimeoutMs = 300000  // 5 minutes
                };
                
                // Add security settings if configured
                if (!string.IsNullOrEmpty(_configuration.KafkaSecurityProtocol) && 
                    _configuration.KafkaSecurityProtocol.ToLower() != "plaintext")
                {
                    config.SecurityProtocol = ParseSecurityProtocol(_configuration.KafkaSecurityProtocol);
                    
                    if (!string.IsNullOrEmpty(_configuration.KafkaSaslMechanism))
                    {
                        config.SaslMechanism = ParseSaslMechanism(_configuration.KafkaSaslMechanism);
                        config.SaslUsername = _configuration.KafkaSaslUsername;
                        config.SaslPassword = _configuration.KafkaSaslPassword;
                    }
                }
                
                _producer = new ProducerBuilder<string, string>(config)
                    .SetErrorHandler((_, e) => HandleKafkaError(e))
                    .SetLogHandler((_, log) => HandleKafkaLog(log))
                    .Build();
                
                _isConnected = true;
                _consecutiveFailures = 0;
                _errorHandler.HandleError("Successfully connected to Kafka", 
                    new Exception("Kafka connection established"));
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError($"Kafka initialization failed: {ex.Message}", ex);
                _isConnected = false;
                throw;
            }
        }
        
        private SecurityProtocol ParseSecurityProtocol(string protocol)
        {
            switch (protocol.ToLower())
            {
                case "ssl": return SecurityProtocol.Ssl;
                case "sasl_plaintext": return SecurityProtocol.SaslPlaintext;
                case "sasl_ssl": return SecurityProtocol.SaslSsl;
                default: return SecurityProtocol.Plaintext;
            }
        }
        
        private SaslMechanism ParseSaslMechanism(string mechanism)
        {
            switch (mechanism.ToLower())
            {
                case "plain": return SaslMechanism.Plain;
                case "scram-sha-256": return SaslMechanism.ScramSha256;
                case "scram-sha-512": return SaslMechanism.ScramSha512;
                default: return SaslMechanism.Plain;
            }
        }
        
        public void WriteToFile(string content)
        {
            lock (_lockObject)
            {
                int attemptCount = 0;
                Exception lastException = null;
                
                while (attemptCount < _configuration.KafkaRetryCount)
                {
                    try
                    {
                        attemptCount++;
                        
                        // Check if producer needs to be recreated
                        if (!_isConnected || _producer == null)
                        {
                            _errorHandler.HandleError($"Reconnecting to Kafka (attempt {attemptCount})", 
                                new Exception("Kafka reconnection"));
                            Reconnect();
                        }
                        
                        // Create message with timestamp as key for ordering
                        var message = new Message<string, string>
                        {
                            Key = DateTime.UtcNow.ToString("O"),
                            Value = content,
                            Timestamp = new Timestamp(DateTimeOffset.UtcNow)
                        };
                        
                        // Send message synchronously to ensure delivery before continuing
                        var deliveryResult = _producer.ProduceAsync(_configuration.KafkaTopicName, message)
                            .GetAwaiter().GetResult();
                        
                        // Check if message was delivered successfully
                        if (deliveryResult.Status == PersistenceStatus.Persisted)
                        {
                            _errorHandler.HandleError(
                                $"Message sent to Kafka topic: {_configuration.KafkaTopicName}, " +
                                $"Partition: {deliveryResult.Partition}, Offset: {deliveryResult.Offset}", 
                                new Exception("Kafka write successful"));
                            
                            // Reset failure count on success
                            _consecutiveFailures = 0;
                            _isConnected = true;
                            return;  // Success - exit method
                        }
                        else
                        {
                            throw new Exception($"Message not persisted. Status: {deliveryResult.Status}");
                        }
                    }
                    catch (ProduceException<string, string> pEx)
                    {
                        lastException = pEx;
                        _errorHandler.HandleError(
                            $"Kafka produce failed (attempt {attemptCount}/{_configuration.KafkaRetryCount}): " +
                            $"Error: {pEx.Error.Reason}, Code: {pEx.Error.Code}", pEx);
                        
                        _isConnected = false;
                        
                        // Wait before retry if not the last attempt
                        if (attemptCount < _configuration.KafkaRetryCount)
                        {
                            Thread.Sleep(_configuration.KafkaRetryDelayMs);
                        }
                    }
                    catch (Exception ex)
                    {
                        lastException = ex;
                        _errorHandler.HandleError(
                            $"Kafka write failed (attempt {attemptCount}/{_configuration.KafkaRetryCount}): {ex.Message}", ex);
                        
                        _isConnected = false;
                        
                        // Wait before retry if not the last attempt
                        if (attemptCount < _configuration.KafkaRetryCount)
                        {
                            Thread.Sleep(_configuration.KafkaRetryDelayMs);
                        }
                    }
                }
                
                // All retries exhausted - trigger critical failure
                _consecutiveFailures++;
                _errorHandler.HandleError(
                    $"CRITICAL: Kafka write failed after {_configuration.KafkaRetryCount} attempts. " +
                    $"Consecutive failures: {_consecutiveFailures}", 
                    lastException ?? new Exception("Kafka write failed"));
                
                // Notify service to shutdown
                OnCriticalFailure?.Invoke(this, new KafkaFailureEventArgs
                {
                    ErrorMessage = $"Kafka write failed after {_configuration.KafkaRetryCount} retries",
                    FailureCount = _consecutiveFailures
                });
                
                // Re-throw to ensure message goes to error queue
                throw new Exception($"Kafka write failed after {_configuration.KafkaRetryCount} attempts", lastException);
            }
        }
        
        private void Reconnect()
        {
            try
            {
                // Dispose old producer
                DisposeProducer();
                
                // Wait before reconnecting
                Thread.Sleep(_configuration.KafkaRetryDelayMs);
                
                // Create new producer
                InitializeProducer();
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Kafka reconnection failed", ex);
                _isConnected = false;
                throw;
            }
        }
        
        private void HandleKafkaError(Error error)
        {
            if (error.IsFatal)
            {
                _isConnected = false;
                _errorHandler.HandleError($"Kafka fatal error: {error.Reason}", 
                    new Exception($"Kafka error code: {error.Code}"));
            }
            else if (error.IsError)
            {
                _errorHandler.HandleError($"Kafka error: {error.Reason}", 
                    new Exception($"Kafka error code: {error.Code}"));
            }
        }
        
        private void HandleKafkaLog(LogMessage log)
        {
            if (log.Level <= SyslogLevel.Warning)
            {
                _errorHandler.HandleError($"Kafka log [{log.Level}]: {log.Message}", 
                    new Exception("Kafka log message"));
            }
        }
        
        private void DisposeProducer()
        {
            try
            {
                if (_producer != null)
                {
                    // Flush any pending messages with timeout
                    _producer.Flush(TimeSpan.FromSeconds(10));
                    _producer.Dispose();
                }
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Error disposing Kafka producer", ex);
            }
            finally
            {
                _producer = null;
                _isConnected = false;
            }
        }
        
        public void Dispose()
        {
            DisposeProducer();
        }
    }
}

// ========== Services/IHeartbeatService.cs ==========
using System;
using System.Threading;

namespace TibcoEmsService.Services
{
    public interface IHeartbeatService : IDisposable
    {
        void StartHeartbeat(CancellationToken cancellationToken, IMessageProcessor messageProcessor);
    }
}

// ========== Services/HeartbeatService.cs ==========
using System;
using System.Threading;
using TIBCO.EMS;
using TibcoEmsService.Configuration;
using Newtonsoft.Json;

namespace TibcoEmsService.Services
{
    public class HeartbeatService : IHeartbeatService
    {
        private readonly IEmsConnectionFactory _connectionFactory;
        private readonly IServiceConfiguration _configuration;
        private readonly IErrorHandler _errorHandler;
        private IConnection _connection;
        private ISession _session;
        private IMessageProducer _heartbeatProducer;
        private volatile bool _connectionLost = false;
        private IMessageProcessor _messageProcessor;
        
        public HeartbeatService(
            IEmsConnectionFactory connectionFactory,
            IServiceConfiguration configuration,
            IErrorHandler errorHandler)
        {
            _connectionFactory = connectionFactory;
            _configuration = configuration;
            _errorHandler = errorHandler;
        }
        
        public void StartHeartbeat(CancellationToken cancellationToken, IMessageProcessor messageProcessor)
        {
            _messageProcessor = messageProcessor;
            
            try
            {
                InitializeConnections();
                
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Check if connection was lost and reconnect if needed
                        if (_connectionLost)
                        {
                            _errorHandler.HandleError("Heartbeat connection lost, attempting to reconnect", 
                                new Exception("Connection lost"));
                            ReconnectHeartbeat();
                        }
                        
                        SendHeartbeat();
                        
                        // Wait for configured interval or until cancelled
                        var waitTime = TimeSpan.FromSeconds(_configuration.HeartbeatIntervalSeconds);
                        if (cancellationToken.WaitHandle.WaitOne(waitTime))
                        {
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        _errorHandler.HandleError("Heartbeat error", ex);
                        
                        // Try to reconnect on next iteration
                        _connectionLost = true;
                    }
                }
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Fatal heartbeat error", ex);
                throw;
            }
        }
        
        private void InitializeConnections()
        {
            _connection = _connectionFactory.CreateConnection();
            
            // Set up exception listener to detect connection issues
            _connection.ExceptionListener = new ExceptionListener(OnConnectionException);
            
            _session = _connectionFactory.CreateSession(_connection);
            _heartbeatProducer = _connectionFactory.CreateProducer(_session, _configuration.HeartbeatQueueName);
            _connectionLost = false;
        }
        
        private void OnConnectionException(Exception exception)
        {
            _connectionLost = true;
            _errorHandler.HandleError("Heartbeat connection exception detected", exception);
        }
        
        private void ReconnectHeartbeat()
        {
            try
            {
                // Clean up old connections
                DisposeConnections();
                
                // Wait a bit before reconnecting
                Thread.Sleep(2000);
                
                // Reinitialize connections
                InitializeConnections();
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Failed to reconnect heartbeat service", ex);
                throw;
            }
        }
        
        private void SendHeartbeat()
        {
            // Get the current message counts
            long totalMessagesRead = _messageProcessor?.GetMessageCount() ?? 0;
            long successfulMessages = _messageProcessor?.GetSuccessfulMessageCount() ?? 0;
            long errorMessages = totalMessagesRead - successfulMessages;
            
            // Check if processing is paused
            bool isPaused = false;
            if (_messageProcessor is MessageProcessor processor)
            {
                isPaused = processor.IsPaused();
            }
            
            var heartbeatData = new
            {
                serviceName = "TibcoEmsProcessorService",
                timestamp = DateTime.UtcNow.ToString("O"),
                autoMode = _configuration.AutoMode,
                status = isPaused ? "paused" : "alive",
                hostname = Environment.MachineName,
                heartbeatIntervalSeconds = _configuration.HeartbeatIntervalSeconds,
                messagesRead = totalMessagesRead,
                messagesProcessedSuccessfully = successfulMessages,
                messagesInError = errorMessages
            };
            
            var jsonContent = JsonConvert.SerializeObject(heartbeatData);
            var message = _session.CreateTextMessage(jsonContent);
            
            _heartbeatProducer.Send(message);
        }
        
        private void DisposeConnections()
        {
            try
            {
                _heartbeatProducer?.Close();
                _session?.Close();
                _connection?.Close();
            }
            catch
            {
                // Best effort cleanup
            }
            finally
            {
                _heartbeatProducer = null;
                _session = null;
                _connection = null;
            }
        }
        
        public void Dispose()
        {
            DisposeConnections();
        }
    }
    
    // Inner class for ExceptionListener
    internal class ExceptionListener : IExceptionListener
    {
        private readonly Action<Exception> _onException;
        
        public ExceptionListener(Action<Exception> onException)
        {
            _onException = onException;
        }
        
        public void OnException(Exception exception)
        {
            _onException?.Invoke(exception);
        }
    }
}

// ========== Services/ICommandService.cs ==========
using System;
using System.Threading;

namespace TibcoEmsService.Services
{
    public interface ICommandService : IDisposable
    {
        void StartCommandListener(
            CancellationToken cancellationToken, 
            CancellationTokenSource serviceCancellation,
            IMessageProcessor messageProcessor);
    }
}

// ========== Services/CommandService.cs ==========
using System;
using System.Threading;
using TIBCO.EMS;
using TibcoEmsService.Configuration;
using Newtonsoft.Json.Linq;

namespace TibcoEmsService.Services
{
    public class CommandService : ICommandService
    {
        private readonly IEmsConnectionFactory _connectionFactory;
        private readonly IServiceConfiguration _configuration;
        private readonly IErrorHandler _errorHandler;
        private IConnection _connection;
        private ISession _session;
        private IMessageConsumer _commandConsumer;
        private IMessageProcessor _messageProcessor;
        private CancellationTokenSource _serviceCancellation;
        
        public CommandService(
            IEmsConnectionFactory connectionFactory,
            IServiceConfiguration configuration,
            IErrorHandler errorHandler)
        {
            _connectionFactory = connectionFactory;
            _configuration = configuration;
            _errorHandler = errorHandler;
        }
        
        public void StartCommandListener(
            CancellationToken cancellationToken,
            CancellationTokenSource serviceCancellation,
            IMessageProcessor messageProcessor)
        {
            _serviceCancellation = serviceCancellation;
            _messageProcessor = messageProcessor;
            
            try
            {
                InitializeConnections();
                
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Check for command messages with timeout
                        var message = _commandConsumer.Receive(TimeSpan.FromSeconds(1));
                        
                        if (message != null)
                        {
                            ProcessCommand(message);
                            message.Acknowledge();
                        }
                    }
                    catch (Exception ex)
                    {
                        _errorHandler.HandleError("Command processing error", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Fatal command service error", ex);
                throw;
            }
        }
        
        private void InitializeConnections()
        {
            _connection = _connectionFactory.CreateConnection();
            _session = _connectionFactory.CreateSession(_connection);
            _commandConsumer = _connectionFactory.CreateConsumer(_session, _configuration.CommandQueueName);
        }
        
        private void ProcessCommand(Message message)
        {
            try
            {
                var textMessage = message as TextMessage;
                if (textMessage == null)
                {
                    _errorHandler.HandleError("Command message is not a TextMessage", 
                        new InvalidOperationException());
                    return;
                }
                
                string commandJson = textMessage.Text;
                _errorHandler.HandleError($"Received command: {commandJson}", 
                    new Exception("Command received"));
                
                // Parse JSON to extract command
                var jsonObject = JObject.Parse(commandJson);
                var command = jsonObject["command"]?.ToString()?.ToLower();
                
                switch (command)
                {
                    case "stop":
                        HandleStopCommand();
                        break;
                        
                    case "wait":
                        HandleWaitCommand();
                        break;
                        
                    case "resume":
                        HandleResumeCommand();
                        break;
                        
                    default:
                        _errorHandler.HandleError($"Unknown command: {command}", 
                            new Exception("Unknown command"));
                        break;
                }
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Failed to process command", ex);
            }
        }
        
        private void HandleStopCommand()
        {
            _errorHandler.HandleError("STOP command received - initiating graceful shutdown", 
                new Exception("Stop command"));
            
            // Request stop which will wait for current message to complete
            if (_messageProcessor is MessageProcessor processor)
            {
                processor.RequestStop();
            }
            
            // Log final counts and initiate shutdown
            var totalCount = _messageProcessor?.GetMessageCount() ?? 0;
            var successfulCount = _messageProcessor?.GetSuccessfulMessageCount() ?? 0;
            var errorCount = totalCount - successfulCount;
            
            _errorHandler.HandleError($"Shutting down. Total: {totalCount}, Successful: {successfulCount}, Errors: {errorCount}", 
                new Exception("Stop command execution"));
            
            // Signal service cancellation
            _serviceCancellation?.Cancel();
        }
        
        private void HandleWaitCommand()
        {
            _errorHandler.HandleError("WAIT command received - pausing message processing", 
                new Exception("Wait command"));
            
            // PauseProcessing will wait for current message to complete
            if (_messageProcessor is MessageProcessor processor)
            {
                // Run pause in a separate thread to avoid blocking command processing
                Task.Run(() => processor.PauseProcessing());
            }
        }
        
        private void HandleResumeCommand()
        {
            _errorHandler.HandleError("RESUME command received - resuming message processing", 
                new Exception("Resume command"));
            
            if (_messageProcessor is MessageProcessor processor)
            {
                processor.ResumeProcessing();
            }
        }
        
        public void Dispose()
        {
            _commandConsumer?.Close();
            _session?.Close();
            _connection?.Close();
        }
    }
}

// ========== Services/IErrorHandler.cs ==========
namespace TibcoEmsService.Services
{
    public interface IErrorHandler
    {
        void HandleError(string context, System.Exception exception);
    }
}

// ========== Services/ErrorHandler.cs ==========
using System;
using System.IO;
using System.Diagnostics;

namespace TibcoEmsService.Services
{
    public class ErrorHandler : IErrorHandler
    {
        private readonly string _logPath;
        private readonly object _lockObject = new object();
        
        public ErrorHandler()
        {
            _logPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "TibcoEmsService",
                "logs"
            );
            
            EnsureLogDirectoryExists();
        }
        
        private void EnsureLogDirectoryExists()
        {
            if (!Directory.Exists(_logPath))
            {
                Directory.CreateDirectory(_logPath);
            }
        }
        
        public void HandleError(string context, Exception exception)
        {
            try
            {
                lock (_lockObject)
                {
                    string logFile = Path.Combine(_logPath, $"service_{DateTime.Now:yyyyMMdd}.log");
                    string logEntry = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {context}: {exception.Message}\r\n{exception.StackTrace}\r\n---\r\n";
                    
                    File.AppendAllText(logFile, logEntry);
                    
                    // Also log to Windows Event Log if available
                    try
                    {
                        if (!EventLog.SourceExists("TibcoEmsService"))
                        {
                            EventLog.CreateEventSource("TibcoEmsService", "Application");
                        }
                        
                        EventLog.WriteEntry("TibcoEmsService", 
                            $"{context}: {exception.Message}", 
                            EventLogEntryType.Error);
                    }
                    catch
                    {
                        // Event log might not be available
                    }
                }
            }
            catch
            {
                // Last resort - prevent error handler from throwing
            }
        }
    }
}

// ========== App.config ==========
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <startup>
    <supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.8" />
  </startup>
  
  <appSettings>
    <!-- TIBCO EMS Configuration -->
    <add key="EmsServerUrl" value="tcp://localhost:7222" />
    <add key="Username" value="admin" />
    <add key="Password" value="admin" />
    <add key="InputQueueName" value="input.queue" />
    <add key="ErrorQueueName" value="error.queue" />
    <add key="HeartbeatQueueName" value="heartbeat.queue" />
    <add key="CommandTopicName" value="command.topic" />
    <add key="OutputDirectory" value="C:\EmsOutput" />
    <add key="HeartbeatIntervalSeconds" value="60" />
    
    <!-- Kafka Configuration -->
    <add key="KafkaBootstrapServers" value="localhost:9092" />
    <add key="KafkaTopicName" value="ems-output-topic" />
    <add key="KafkaClientId" value="TibcoEmsProcessor" />
    <add key="KafkaSecurityProtocol" value="Plaintext" />
    <add key="KafkaSaslMechanism" value="" />
    <add key="KafkaSaslUsername" value="" />
    <add key="KafkaSaslPassword" value="" />
    <add key="KafkaRetryCount" value="5" />
    <add key="KafkaRetryDelayMs" value="2000" />
  </appSettings>
  
  <system.diagnostics>
    <trace autoflush="true">
      <listeners>
        <add name="textWriterTraceListener" 
             type="System.Diagnostics.TextWriterTraceListener" 
             initializeData="C:\ProgramData\TibcoEmsService\logs\trace.log" />
      </listeners>
    </trace>
  </system.diagnostics>
</configuration>

// ========== packages.config ==========
<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="Autofac" version="6.5.0" targetFramework="net48" />
  <package id="Newtonsoft.Json" version="13.0.3" targetFramework="net48" />
  <package id="Confluent.Kafka" version="2.3.0" targetFramework="net48" />
  <package id="librdkafka.redist" version="2.3.0" targetFramework="net48" />
  <package id="System.Diagnostics.DiagnosticSource" version="4.7.1" targetFramework="net48" />
  <package id="System.Memory" version="4.5.5" targetFramework="net48" />
  <package id="System.Runtime.CompilerServices.Unsafe" version="4.5.3" targetFramework="net48" />
</packages>

// ========== packages.config ==========
<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="Autofac" version="6.5.0" targetFramework="net48" />
  <package id="Newtonsoft.Json" version="13.0.3" targetFramework="net48" />
  <package id="System.Diagnostics.DiagnosticSource" version="4.7.1" targetFramework="net48" />
  <package id="System.Memory" version="4.5.5" targetFramework="net48" />
  <package id="System.Runtime.CompilerServices.Unsafe" version="4.5.3" targetFramework="net48" />
  <!-- IBM MQ .NET Client - Install via NuGet or IBM MQ installation -->
  <package id="IBMMQDotnetClient" version="9.3.0.0" targetFramework="net48" />
</packages>

// ========== README.md ==========
# TIBCO EMS to IBM MQ Bridge Service

## Overview
A Windows Service that reads TextMessages from TIBCO EMS, transforms them to JSON, and writes them to IBM MQ. The service follows SOLID principles and uses Autofac for dependency injection.

## Features
- Continuously reads TextMessages from TIBCO EMS queue
- Transforms messages to JSON format
- Writes JSON messages to IBM MQ queue
- Sends failed messages to error queue with acknowledgment
- Sends heartbeat messages every 60 seconds (configurable)
- Remote control via command queue (stop/wait/resume)
- Graceful shutdown handling
- Comprehensive error logging
- Automatic reconnection for both EMS and MQ connections
- Thread-safe message counting

## Architecture

### SOLID Principles Implementation
1. **Single Responsibility**: Each class has one reason to change
2. **Open/Closed**: Extension through interfaces, not modification
3. **Liskov Substitution**: All implementations properly substitute interfaces
4. **Interface Segregation**: Small, focused interfaces
5. **Dependency Inversion**: Depend on abstractions via constructor injection

### Data Flow
```
TIBCO EMS → Read Message → Transform to JSON → Send to IBM MQ
     ↑                                              
     |                                              
Command Queue (stop/wait/resume)                    
     |
Heartbeat Queue (monitoring)
     |
Error Queue (failed messages)
```

## Installation

### Prerequisites
1. .NET Framework 4.8
2. TIBCO EMS .NET Client Libraries
3. IBM MQ .NET Client (Install via NuGet: `IBMMQDotnetClient`)
4. Administrator privileges for service installation

### Installation Steps
1. Build the project in Release mode
2. Install required NuGet packages
3. Reference TIBCO EMS .NET client DLL
4. Run as Administrator:
   ```
   installutil.exe TibcoEmsService.exe
   ```

## Configuration

Edit App.config to configure the service:

### TIBCO EMS Settings
- `EmsServerUrl`: EMS server URL (default: tcp://localhost:7222)
- `Username`: EMS username
- `Password`: EMS password
- `InputQueueName`: Queue to read messages from
- `ErrorQueueName`: Queue for failed messages
- `HeartbeatQueueName`: Queue for heartbeat messages
- `CommandQueueName`: Queue for control commands

### IBM MQ Settings
- `MqQueueManager`: Queue manager name (default: QM1)
- `MqChannel`: Server connection channel
- `MqConnectionName`: Host and port (e.g., localhost(1414))
- `MqOutputQueue`: Target queue for messages
- `MqUserId`: MQ username (optional)
- `MqPassword`: MQ password (optional)

### Service Settings
- `HeartbeatIntervalSeconds`: Heartbeat frequency (default: 60)
- `autoMode`: Pass "y" or "n" as command line argument

## Usage

### Starting the Service
```bash
# Start with autoMode=y
sc start TibcoEmsProcessorService y

# Or using net command
net start TibcoEmsProcessorService
```

### Control Commands
Send JSON messages to the command queue:

**Pause processing:**
```json
{"command": "wait"}
```

**Resume processing:**
```json
{"command": "resume"}
```

**Stop service gracefully:**
```json
{"command": "stop"}
```

### Heartbeat Message Format
```json
{
  "serviceName": "TibcoEmsProcessorService",
  "timestamp": "2025-08-28T10:30:00.000Z",
  "autoMode": "y",
  "status": "alive",  // or "paused"
  "hostname": "SERVER01",
  "heartbeatIntervalSeconds": 60,
  "messagesRead": 1547
}
```

### Monitoring
- Check heartbeat messages for service status
- Monitor error queue for failed messages
- Review logs at: `C:\ProgramData\TibcoEmsService\logs\`
- Check Windows Event Log for critical errors

## Uninstallation
```bash
installutil.exe /u TibcoEmsService.exe
```

## Error Handling
- All errors logged to file and Windows Event Log
- Failed messages sent to error queue with metadata
- Service continues operation on non-fatal errors
- Automatic reconnection for lost connections
- Graceful shutdown ensures no message loss

## Thread Safety
- Message counting using Interlocked operations
- Volatile flags for cross-thread communication
- Thread-safe file writing with locks
- Proper cancellation token handling
