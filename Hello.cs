// ========== Program.cs ==========
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
            
            // Register MqWriter for IBM MQ output
            builder.RegisterType<MqWriter>()
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
        string CommandQueueName { get; }
        string OutputDirectory { get; }
        string AutoMode { get; }
        int HeartbeatIntervalSeconds { get; }
        
        // IBM MQ Configuration
        string MqQueueManager { get; }
        string MqChannel { get; }
        string MqConnectionName { get; }
        string MqOutputQueue { get; }
        string MqUserId { get; }
        string MqPassword { get; }
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
        public string CommandQueueName { get; private set; }
        public string OutputDirectory { get; private set; }
        public string AutoMode { get; private set; }
        public int HeartbeatIntervalSeconds { get; private set; }
        
        // IBM MQ Configuration
        public string MqQueueManager { get; private set; }
        public string MqChannel { get; private set; }
        public string MqConnectionName { get; private set; }
        public string MqOutputQueue { get; private set; }
        public string MqUserId { get; private set; }
        public string MqPassword { get; private set; }
        
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
            CommandQueueName = ConfigurationManager.AppSettings["CommandQueueName"] ?? "command.queue";
            OutputDirectory = ConfigurationManager.AppSettings["OutputDirectory"] ?? @"C:\EmsOutput";
            AutoMode = args.Length > 0 && args[0].ToLower() == "y" ? "y" : "n";
            
            // Parse heartbeat interval with default of 60 seconds
            string heartbeatInterval = ConfigurationManager.AppSettings["HeartbeatIntervalSeconds"] ?? "60";
            if (!int.TryParse(heartbeatInterval, out int interval) || interval <= 0)
            {
                interval = 60; // Default to 60 seconds if invalid
            }
            HeartbeatIntervalSeconds = interval;
            
            // IBM MQ Configuration
            MqQueueManager = ConfigurationManager.AppSettings["MqQueueManager"] ?? "QM1";
            MqChannel = ConfigurationManager.AppSettings["MqChannel"] ?? "DEV.APP.SVRCONN";
            MqConnectionName = ConfigurationManager.AppSettings["MqConnectionName"] ?? "localhost(1414)";
            MqOutputQueue = ConfigurationManager.AppSettings["MqOutputQueue"] ?? "OUTPUT.QUEUE";
            MqUserId = ConfigurationManager.AppSettings["MqUserId"] ?? "";
            MqPassword = ConfigurationManager.AppSettings["MqPassword"] ?? "";
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
        }
        
        public void StartProcessing(CancellationToken cancellationToken)
        {
            try
            {
                InitializeConnections();
                
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Check if processing is paused
                        if (_isPaused)
                        {
                            // Sleep briefly and continue loop to check cancellation
                            Thread.Sleep(100);
                            continue;
                        }
                        
                        var message = _consumer.Receive(TimeSpan.FromSeconds(1));
                        
                        if (message != null)
                        {
                            _isProcessing = true;
                            ProcessMessage(message);
                            _isProcessing = false;
                        }
                    }
                    catch (Exception ex)
                    {
                        _errorHandler.HandleError("Message processing error", ex);
                        _isProcessing = false;
                    }
                }
                
                // Wait for current message to complete
                while (_isProcessing)
                {
                    Thread.Sleep(100);
                }
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
                
                // Write to MQ
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
                // Send to error queue
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
        
        public void Dispose()
        {
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
namespace TibcoEmsService.Services
{
    public interface IFileWriter
    {
        void WriteToFile(string content);
    }
}

// ========== Services/MqWriter.cs ==========
using System;
using System.Collections;
using IBM.WMQ;
using TibcoEmsService.Configuration;

namespace TibcoEmsService.Services
{
    public class MqWriter : IFileWriter, IDisposable
    {
        private readonly IServiceConfiguration _configuration;
        private readonly IErrorHandler _errorHandler;
        private readonly object _lockObject = new object();
        private MQQueueManager _queueManager;
        private MQQueue _outputQueue;
        private volatile bool _isConnected = false;
        
        public MqWriter(IServiceConfiguration configuration, IErrorHandler errorHandler)
        {
            _configuration = configuration;
            _errorHandler = errorHandler;
            InitializeConnection();
        }
        
        private void InitializeConnection()
        {
            try
            {
                // Set up connection properties
                Hashtable properties = new Hashtable();
                properties.Add(MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_MANAGED);
                properties.Add(MQC.HOST_NAME_PROPERTY, GetHostFromConnectionName());
                properties.Add(MQC.PORT_PROPERTY, GetPortFromConnectionName());
                properties.Add(MQC.CHANNEL_PROPERTY, _configuration.MqChannel);
                
                if (!string.IsNullOrEmpty(_configuration.MqUserId))
                {
                    properties.Add(MQC.USER_ID_PROPERTY, _configuration.MqUserId);
                    properties.Add(MQC.PASSWORD_PROPERTY, _configuration.MqPassword);
                }
                
                // Connect to queue manager
                _queueManager = new MQQueueManager(_configuration.MqQueueManager, properties);
                
                // Open output queue for writing
                int openOptions = MQC.MQOO_OUTPUT | MQC.MQOO_FAIL_IF_QUIESCING;
                _outputQueue = _queueManager.AccessQueue(_configuration.MqOutputQueue, openOptions);
                
                _isConnected = true;
                _errorHandler.HandleError("Successfully connected to IBM MQ", 
                    new Exception("MQ Connection established"));
            }
            catch (MQException mqEx)
            {
                _errorHandler.HandleError($"MQ Connection failed: {mqEx.Message} (Reason: {mqEx.ReasonCode})", 
                    mqEx);
                _isConnected = false;
                throw;
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("Failed to initialize MQ connection", ex);
                _isConnected = false;
                throw;
            }
        }
        
        private string GetHostFromConnectionName()
        {
            // Parse host from connection string like "localhost(1414)"
            var connName = _configuration.MqConnectionName;
            var index = connName.IndexOf('(');
            return index > 0 ? connName.Substring(0, index) : connName;
        }
        
        private int GetPortFromConnectionName()
        {
            // Parse port from connection string like "localhost(1414)"
            var connName = _configuration.MqConnectionName;
            var startIndex = connName.IndexOf('(');
            var endIndex = connName.IndexOf(')');
            
            if (startIndex > 0 && endIndex > startIndex)
            {
                var portStr = connName.Substring(startIndex + 1, endIndex - startIndex - 1);
                if (int.TryParse(portStr, out int port))
                {
                    return port;
                }
            }
            return 1414; // Default MQ port
        }
        
        public void WriteToFile(string content)
        {
            lock (_lockObject)
            {
                try
                {
                    // Check connection and reconnect if necessary
                    if (!_isConnected || !IsConnectionValid())
                    {
                        Reconnect();
                    }
                    
                    // Create MQ message
                    MQMessage message = new MQMessage();
                    message.WriteString(content);
                    message.Format = MQC.MQFMT_STRING;
                    message.CharacterSet = 1208; // UTF-8
                    
                    // Set message properties
                    message.ApplicationIdData = "TibcoEmsProcessor";
                    message.ApplicationOriginData = Environment.MachineName;
                    
                    // Put message options
                    MQPutMessageOptions pmo = new MQPutMessageOptions();
                    pmo.Options = MQC.MQPMO_NO_SYNCPOINT | MQC.MQPMO_FAIL_IF_QUIESCING;
                    
                    // Send message to queue
                    _outputQueue.Put(message, pmo);
                    
                    _errorHandler.HandleError($"Message sent to MQ queue: {_configuration.MqOutputQueue}", 
                        new Exception("MQ Write successful"));
                }
                catch (MQException mqEx)
                {
                    _errorHandler.HandleError($"MQ Write failed: {mqEx.Message} (Reason: {mqEx.ReasonCode})", 
                        mqEx);
                    _isConnected = false;
                    throw;
                }
                catch (Exception ex)
                {
                    _errorHandler.HandleError("Failed to write to MQ", ex);
                    _isConnected = false;
                    throw;
                }
            }
        }
        
        private bool IsConnectionValid()
        {
            try
            {
                // Check if queue manager is connected
                return _queueManager != null && _queueManager.IsConnected;
            }
            catch
            {
                return false;
            }
        }
        
        private void Reconnect()
        {
            try
            {
                _errorHandler.HandleError("Attempting to reconnect to IBM MQ", 
                    new Exception("MQ Reconnection"));
                
                // Clean up existing connections
                CloseConnections();
                
                // Wait before reconnecting
                System.Threading.Thread.Sleep(2000);
                
                // Reinitialize connection
                InitializeConnection();
            }
            catch (Exception ex)
            {
                _errorHandler.HandleError("MQ reconnection failed", ex);
                throw;
            }
        }
        
        private void CloseConnections()
        {
            try
            {
                _outputQueue?.Close();
            }
            catch { }
            
            try
            {
                _queueManager?.Disconnect();
            }
            catch { }
            
            _outputQueue = null;
            _queueManager = null;
            _isConnected = false;
        }
        
        public void Dispose()
        {
            CloseConnections();
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
            
            // First pause processing
            if (_messageProcessor is MessageProcessor processor)
            {
                processor.PauseProcessing();
            }
            
            // Wait for current message to complete
            Thread.Sleep(2000);
            
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
            
            if (_messageProcessor is MessageProcessor processor)
            {
                processor.PauseProcessing();
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

// ========== ProjectInstaller.cs ==========
using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace TibcoEmsService
{
    [RunInstaller(true)]
    public class ProjectInstaller : Installer
    {
        private ServiceProcessInstaller serviceProcessInstaller;
        private ServiceInstaller serviceInstaller;
        
        public ProjectInstaller()
        {
            serviceProcessInstaller = new ServiceProcessInstaller();
            serviceInstaller = new ServiceInstaller();
            
            serviceProcessInstaller.Account = ServiceAccount.LocalSystem;
            serviceProcessInstaller.Password = null;
            serviceProcessInstaller.Username = null;
            
            serviceInstaller.ServiceName = "TibcoEmsProcessorService";
            serviceInstaller.DisplayName = "TIBCO EMS Message Processor Service";
            serviceInstaller.Description = "Processes messages from TIBCO EMS, converts to JSON and sends to IBM MQ";
            serviceInstaller.StartType = ServiceStartMode.Automatic;
            
            Installers.AddRange(new Installer[] {
                serviceProcessInstaller,
                serviceInstaller
            });
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
    <add key="CommandQueueName" value="command.queue" />
    <add key="OutputDirectory" value="C:\EmsOutput" />
    <add key="HeartbeatIntervalSeconds" value="60" />
    
    <!-- IBM MQ Configuration -->
    <add key="MqQueueManager" value="QM1" />
    <add key="MqChannel" value="DEV.APP.SVRCONN" />
    <add key="MqConnectionName" value="localhost(1414)" />
    <add key="MqOutputQueue" value="OUTPUT.QUEUE" />
    <add key="MqUserId" value="" />
    <add key="MqPassword" value="" />
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
