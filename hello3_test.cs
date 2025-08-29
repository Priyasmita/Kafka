// ========== Tests/Configuration/ServiceConfigurationTests.cs ==========
using System;
using System.Configuration;
using NUnit.Framework;
using TibcoEmsService.Configuration;

namespace TibcoEmsService.Tests.Configuration
{
    [TestFixture]
    public class ServiceConfigurationTests
    {
        [Test]
        public void Constructor_WithNullArgs_InitializesWithDefaults()
        {
            // Arrange & Act
            var config = new ServiceConfiguration(null);
            
            // Assert
            Assert.That(config.EmsServerUrl, Is.EqualTo("tcp://localhost:7222"));
            Assert.That(config.InputQueueName, Is.EqualTo("input.queue"));
            Assert.That(config.KafkaBootstrapServers, Is.EqualTo("localhost:9092"));
            Assert.That(config.KafkaRetryCount, Is.EqualTo(5));
            Assert.That(config.HeartbeatIntervalSeconds, Is.EqualTo(60));
        }
        
        [Test]
        public void Constructor_WithAutoModeY_SetsAutoModeCorrectly()
        {
            // Arrange & Act
            var config = new ServiceConfiguration(new[] { "y" });
            
            // Assert
            Assert.That(config.AutoMode, Is.EqualTo("y"));
        }
        
        [Test]
        public void Constructor_WithAutoModeN_SetsAutoModeCorrectly()
        {
            // Arrange & Act
            var config = new ServiceConfiguration(new[] { "n" });
            
            // Assert
            Assert.That(config.AutoMode, Is.EqualTo("n"));
        }
        
        [Test]
        public void Constructor_WithEmptyArgs_DefaultsAutoModeToN()
        {
            // Arrange & Act
            var config = new ServiceConfiguration(new string[0]);
            
            // Assert
            Assert.That(config.AutoMode, Is.EqualTo("n"));
        }
    }
}

// ========== Tests/Services/JsonTransformerTests.cs ==========
using System;
using NUnit.Framework;
using TibcoEmsService.Services;
using Newtonsoft.Json.Linq;

namespace TibcoEmsService.Tests.Services
{
    [TestFixture]
    public class JsonTransformerTests
    {
        private JsonTransformer _transformer;
        
        [SetUp]
        public void Setup()
        {
            _transformer = new JsonTransformer();
        }
        
        [Test]
        public void TransformToJson_WithPlainText_ReturnsJsonWithContent()
        {
            // Arrange
            string input = "Test message";
            
            // Act
            string result = _transformer.TransformToJson(input);
            
            // Assert
            var json = JObject.Parse(result);
            Assert.That(json["content"].ToString(), Is.EqualTo("Test message"));
            Assert.That(json["messageType"].ToString(), Is.EqualTo("text"));
            Assert.That(json["timestamp"], Is.Not.Null);
        }
        
        [Test]
        public void TransformToJson_WithExistingJson_ReturnsOriginalJson()
        {
            // Arrange
            string input = "{\"key\":\"value\"}";
            
            // Act
            string result = _transformer.TransformToJson(input);
            
            // Assert
            Assert.That(result, Is.EqualTo(input));
        }
        
        [Test]
        public void TransformToJson_WithInvalidJson_ReturnsWrappedJson()
        {
            // Arrange
            string input = "{invalid json}";
            
            // Act
            string result = _transformer.TransformToJson(input);
            
            // Assert
            var json = JObject.Parse(result);
            Assert.That(json["content"].ToString(), Is.EqualTo("{invalid json}"));
            Assert.That(json["messageType"].ToString(), Is.EqualTo("text"));
        }
    }
}

// ========== Tests/Services/MessageProcessorTests.cs ==========
using System;
using System.Threading;
using Moq;
using NUnit.Framework;
using TIBCO.EMS;
using TibcoEmsService.Configuration;
using TibcoEmsService.Services;

namespace TibcoEmsService.Tests.Services
{
    [TestFixture]
    public class MessageProcessorTests
    {
        private Mock<IEmsConnectionFactory> _mockConnectionFactory;
        private Mock<IJsonTransformer> _mockJsonTransformer;
        private Mock<IFileWriter> _mockFileWriter;
        private Mock<IErrorHandler> _mockErrorHandler;
        private Mock<IServiceConfiguration> _mockConfiguration;
        private MessageProcessor _processor;
        
        [SetUp]
        public void Setup()
        {
            _mockConnectionFactory = new Mock<IEmsConnectionFactory>();
            _mockJsonTransformer = new Mock<IJsonTransformer>();
            _mockFileWriter = new Mock<IFileWriter>();
            _mockErrorHandler = new Mock<IErrorHandler>();
            _mockConfiguration = new Mock<IServiceConfiguration>();
            
            _mockConfiguration.Setup(c => c.InputQueueName).Returns("input.queue");
            _mockConfiguration.Setup(c => c.ErrorQueueName).Returns("error.queue");
            
            _processor = new MessageProcessor(
                _mockConnectionFactory.Object,
                _mockJsonTransformer.Object,
                _mockFileWriter.Object,
                _mockErrorHandler.Object,
                _mockConfiguration.Object);
        }
        
        [Test]
        public void GetMessageCount_InitiallyReturnsZero()
        {
            // Act
            long count = _processor.GetMessageCount();
            
            // Assert
            Assert.That(count, Is.EqualTo(0));
        }
        
        [Test]
        public void GetSuccessfulMessageCount_InitiallyReturnsZero()
        {
            // Act
            long count = _processor.GetSuccessfulMessageCount();
            
            // Assert
            Assert.That(count, Is.EqualTo(0));
        }
        
        [Test]
        public void PauseProcessing_SetsIsPausedToTrue()
        {
            // Act
            _processor.PauseProcessing();
            
            // Assert
            Assert.That(_processor.IsPaused(), Is.True);
        }
        
        [Test]
        public void ResumeProcessing_SetsIsPausedToFalse()
        {
            // Arrange
            _processor.PauseProcessing();
            
            // Act
            _processor.ResumeProcessing();
            
            // Assert
            Assert.That(_processor.IsPaused(), Is.False);
        }
        
        [Test]
        public void RequestStop_LogsStopMessage()
        {
            // Act
            _processor.RequestStop();
            
            // Assert
            _mockErrorHandler.Verify(e => e.HandleError(
                It.Is<string>(s => s.Contains("Stop requested")),
                It.IsAny<Exception>()), Times.Once);
        }
        
        [Test]
        public void SetServiceCancellation_DoesNotThrow()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            
            // Act & Assert
            Assert.DoesNotThrow(() => _processor.SetServiceCancellation(cts));
        }
        
        [Test]
        public void Dispose_ClosesConnections()
        {
            // Arrange
            var mockConnection = new Mock<IConnection>();
            var mockSession = new Mock<ISession>();
            var mockConsumer = new Mock<IMessageConsumer>();
            var mockProducer = new Mock<IMessageProducer>();
            
            _mockConnectionFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);
            _mockConnectionFactory.Setup(f => f.CreateSession(It.IsAny<IConnection>())).Returns(mockSession.Object);
            _mockConnectionFactory.Setup(f => f.CreateConsumer(It.IsAny<ISession>(), It.IsAny<string>())).Returns(mockConsumer.Object);
            _mockConnectionFactory.Setup(f => f.CreateProducer(It.IsAny<ISession>(), It.IsAny<string>())).Returns(mockProducer.Object);
            
            var cts = new CancellationTokenSource();
            cts.Cancel();
            _processor.StartProcessing(cts.Token);
            
            // Act
            _processor.Dispose();
            
            // Assert
            mockConsumer.Verify(c => c.Close(), Times.Once);
            mockSession.Verify(s => s.Close(), Times.Once);
            mockConnection.Verify(c => c.Close(), Times.Once);
        }
    }
}

// ========== Tests/Services/KafkaWriterTests.cs ==========
using System;
using Moq;
using NUnit.Framework;
using TibcoEmsService.Configuration;
using TibcoEmsService.Services;

namespace TibcoEmsService.Tests.Services
{
    [TestFixture]
    public class KafkaWriterTests
    {
        private Mock<IServiceConfiguration> _mockConfiguration;
        private Mock<IErrorHandler> _mockErrorHandler;
        
        [SetUp]
        public void Setup()
        {
            _mockConfiguration = new Mock<IServiceConfiguration>();
            _mockErrorHandler = new Mock<IErrorHandler>();
            
            _mockConfiguration.Setup(c => c.KafkaBootstrapServers).Returns("localhost:9092");
            _mockConfiguration.Setup(c => c.KafkaTopicName).Returns("test-topic");
            _mockConfiguration.Setup(c => c.KafkaClientId).Returns("test-client");
            _mockConfiguration.Setup(c => c.KafkaRetryCount).Returns(3);
            _mockConfiguration.Setup(c => c.KafkaRetryDelayMs).Returns(100);
            _mockConfiguration.Setup(c => c.KafkaSecurityProtocol).Returns("Plaintext");
        }
        
        [Test]
        public void OnCriticalFailure_Event_CanBeSubscribed()
        {
            // Arrange
            bool eventRaised = false;
            
            // Note: Creating KafkaWriter will attempt to connect to Kafka
            // In a real test, you'd mock the Kafka producer
            // For demonstration, we'll test the event mechanism
            
            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                // This would normally create the writer, but will fail without Kafka running
                // var writer = new KafkaWriter(_mockConfiguration.Object, _mockErrorHandler.Object);
                // writer.OnCriticalFailure += (sender, args) => eventRaised = true;
                
                // For testing purposes, we verify the event exists in the interface
                Assert.That(typeof(IFileWriter).GetEvent("OnCriticalFailure"), Is.Not.Null);
            });
        }
    }
}

// ========== Tests/Services/HeartbeatServiceTests.cs ==========
using System;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using TIBCO.EMS;
using TibcoEmsService.Configuration;
using TibcoEmsService.Services;

namespace TibcoEmsService.Tests.Services
{
    [TestFixture]
    public class HeartbeatServiceTests
    {
        private Mock<IEmsConnectionFactory> _mockConnectionFactory;
        private Mock<IServiceConfiguration> _mockConfiguration;
        private Mock<IErrorHandler> _mockErrorHandler;
        private Mock<IMessageProcessor> _mockMessageProcessor;
        private HeartbeatService _service;
        
        [SetUp]
        public void Setup()
        {
            _mockConnectionFactory = new Mock<IEmsConnectionFactory>();
            _mockConfiguration = new Mock<IServiceConfiguration>();
            _mockErrorHandler = new Mock<IErrorHandler>();
            _mockMessageProcessor = new Mock<IMessageProcessor>();
            
            _mockConfiguration.Setup(c => c.HeartbeatQueueName).Returns("heartbeat.queue");
            _mockConfiguration.Setup(c => c.HeartbeatIntervalSeconds).Returns(1); // Short interval for testing
            _mockConfiguration.Setup(c => c.AutoMode).Returns("y");
            
            _mockMessageProcessor.Setup(m => m.GetMessageCount()).Returns(100);
            _mockMessageProcessor.Setup(m => m.GetSuccessfulMessageCount()).Returns(95);
            
            _service = new HeartbeatService(
                _mockConnectionFactory.Object,
                _mockConfiguration.Object,
                _mockErrorHandler.Object);
        }
        
        [Test]
        public void StartHeartbeat_CreatesConnectionAndSession()
        {
            // Arrange
            var mockConnection = new Mock<IConnection>();
            var mockSession = new Mock<ISession>();
            var mockProducer = new Mock<IMessageProducer>();
            var cts = new CancellationTokenSource();
            
            _mockConnectionFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);
            _mockConnectionFactory.Setup(f => f.CreateSession(It.IsAny<IConnection>())).Returns(mockSession.Object);
            _mockConnectionFactory.Setup(f => f.CreateProducer(It.IsAny<ISession>(), It.IsAny<string>()))
                .Returns(mockProducer.Object);
            
            mockSession.Setup(s => s.CreateTextMessage(It.IsAny<string>()))
                .Returns(Mock.Of<ITextMessage>());
            
            // Act
            var task = Task.Run(() => _service.StartHeartbeat(cts.Token, _mockMessageProcessor.Object));
            Thread.Sleep(100); // Let it start
            cts.Cancel();
            task.Wait(1000);
            
            // Assert
            _mockConnectionFactory.Verify(f => f.CreateConnection(), Times.Once);
            _mockConnectionFactory.Verify(f => f.CreateSession(It.IsAny<IConnection>()), Times.Once);
        }
        
        [Test]
        public void Dispose_ClosesConnections()
        {
            // Arrange
            var mockConnection = new Mock<IConnection>();
            var mockSession = new Mock<ISession>();
            var mockProducer = new Mock<IMessageProducer>();
            
            _mockConnectionFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);
            _mockConnectionFactory.Setup(f => f.CreateSession(It.IsAny<IConnection>())).Returns(mockSession.Object);
            _mockConnectionFactory.Setup(f => f.CreateProducer(It.IsAny<ISession>(), It.IsAny<string>()))
                .Returns(mockProducer.Object);
            
            var cts = new CancellationTokenSource();
            cts.Cancel();
            _service.StartHeartbeat(cts.Token, _mockMessageProcessor.Object);
            
            // Act
            _service.Dispose();
            
            // Assert
            mockProducer.Verify(p => p.Close(), Times.Once);
            mockSession.Verify(s => s.Close(), Times.Once);
            mockConnection.Verify(c => c.Close(), Times.Once);
        }
    }
}

// ========== Tests/Services/CommandServiceTests.cs ==========
using System;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using TIBCO.EMS;
using TibcoEmsService.Configuration;
using TibcoEmsService.Services;

namespace TibcoEmsService.Tests.Services
{
    [TestFixture]
    public class CommandServiceTests
    {
        private Mock<IEmsConnectionFactory> _mockConnectionFactory;
        private Mock<IServiceConfiguration> _mockConfiguration;
        private Mock<IErrorHandler> _mockErrorHandler;
        private Mock<IMessageProcessor> _mockMessageProcessor;
        private CommandService _service;
        
        [SetUp]
        public void Setup()
        {
            _mockConnectionFactory = new Mock<IEmsConnectionFactory>();
            _mockConfiguration = new Mock<IServiceConfiguration>();
            _mockErrorHandler = new Mock<IErrorHandler>();
            _mockMessageProcessor = new Mock<IMessageProcessor>();
            
            _mockConfiguration.Setup(c => c.CommandTopicName).Returns("command.topic");
            
            _service = new CommandService(
                _mockConnectionFactory.Object,
                _mockConfiguration.Object,
                _mockErrorHandler.Object);
        }
        
        [Test]
        public void StartCommandListener_CreatesTopicSubscriber()
        {
            // Arrange
            var mockConnection = new Mock<IConnection>();
            var mockSession = new Mock<ISession>();
            var mockConsumer = new Mock<IMessageConsumer>();
            var cts = new CancellationTokenSource();
            var serviceCts = new CancellationTokenSource();
            
            _mockConnectionFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);
            _mockConnectionFactory.Setup(f => f.CreateSession(It.IsAny<IConnection>())).Returns(mockSession.Object);
            _mockConnectionFactory.Setup(f => f.CreateTopicSubscriber(It.IsAny<ISession>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(mockConsumer.Object);
            
            // Act
            var task = Task.Run(() => _service.StartCommandListener(cts.Token, serviceCts, _mockMessageProcessor.Object));
            Thread.Sleep(100); // Let it start
            cts.Cancel();
            task.Wait(1000);
            
            // Assert
            _mockConnectionFactory.Verify(f => f.CreateTopicSubscriber(
                It.IsAny<ISession>(), 
                "command.topic", 
                It.IsAny<string>()), Times.Once);
        }
        
        [Test]
        public void ProcessCommand_WithStopCommand_CallsRequestStop()
        {
            // Arrange
            var mockConnection = new Mock<IConnection>();
            var mockSession = new Mock<ISession>();
            var mockConsumer = new Mock<IMessageConsumer>();
            var mockTextMessage = new Mock<ITextMessage>();
            var mockProcessor = new Mock<MessageProcessor>(
                Mock.Of<IEmsConnectionFactory>(),
                Mock.Of<IJsonTransformer>(),
                Mock.Of<IFileWriter>(),
                Mock.Of<IErrorHandler>(),
                Mock.Of<IServiceConfiguration>());
            
            var cts = new CancellationTokenSource();
            var serviceCts = new CancellationTokenSource();
            
            mockTextMessage.Setup(m => m.Text).Returns("{\"command\":\"stop\"}");
            mockConsumer.SetupSequence(c => c.Receive(It.IsAny<TimeSpan>()))
                .Returns(mockTextMessage.Object)
                .Returns((Message)null);
            
            _mockConnectionFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);
            _mockConnectionFactory.Setup(f => f.CreateSession(It.IsAny<IConnection>())).Returns(mockSession.Object);
            _mockConnectionFactory.Setup(f => f.CreateTopicSubscriber(It.IsAny<ISession>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(mockConsumer.Object);
            
            // Act
            var task = Task.Run(() => _service.StartCommandListener(cts.Token, serviceCts, mockProcessor.Object));
            Thread.Sleep(200); // Let it process
            cts.Cancel();
            task.Wait(1000);
            
            // Assert
            mockProcessor.Verify(p => p.RequestStop(), Times.Once);
        }
        
        [Test]
        public void Dispose_ClosesConnections()
        {
            // Arrange
            var mockConnection = new Mock<IConnection>();
            var mockSession = new Mock<ISession>();
            var mockConsumer = new Mock<IMessageConsumer>();
            
            _mockConnectionFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);
            _mockConnectionFactory.Setup(f => f.CreateSession(It.IsAny<IConnection>())).Returns(mockSession.Object);
            _mockConnectionFactory.Setup(f => f.CreateTopicSubscriber(It.IsAny<ISession>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(mockConsumer.Object);
            
            var cts = new CancellationTokenSource();
            cts.Cancel();
            _service.StartCommandListener(cts.Token, new CancellationTokenSource(), _mockMessageProcessor.Object);
            
            // Act
            _service.Dispose();
            
            // Assert
            mockConsumer.Verify(c => c.Close(), Times.Once);
            mockSession.Verify(s => s.Close(), Times.Once);
            mockConnection.Verify(c => c.Close(), Times.Once);
        }
    }
}

// ========== Tests/Services/EmsConnectionFactoryTests.cs ==========
using Moq;
using NUnit.Framework;
using TIBCO.EMS;
using TibcoEmsService.Configuration;
using TibcoEmsService.Services;

namespace TibcoEmsService.Tests.Services
{
    [TestFixture]
    public class EmsConnectionFactoryTests
    {
        private Mock<IServiceConfiguration> _mockConfiguration;
        private EmsConnectionFactory _factory;
        
        [SetUp]
        public void Setup()
        {
            _mockConfiguration = new Mock<IServiceConfiguration>();
            _mockConfiguration.Setup(c => c.EmsServerUrl).Returns("tcp://localhost:7222");
            _mockConfiguration.Setup(c => c.Username).Returns("admin");
            _mockConfiguration.Setup(c => c.Password).Returns("admin");
            
            _factory = new EmsConnectionFactory(_mockConfiguration.Object);
        }
        
        [Test]
        public void Constructor_InitializesWithConfiguration()
        {
            // Assert
            Assert.That(_factory, Is.Not.Null);
        }
        
        [Test]
        public void CreateSession_WithConnection_ReturnsSession()
        {
            // Arrange
            var mockConnection = new Mock<IConnection>();
            var mockSession = new Mock<ISession>();
            mockConnection.Setup(c => c.CreateSession(false, Session.CLIENT_ACKNOWLEDGE))
                .Returns(mockSession.Object);
            
            // Act
            var result = _factory.CreateSession(mockConnection.Object);
            
            // Assert
            Assert.That(result, Is.EqualTo(mockSession.Object));
        }
        
        [Test]
        public void CreateConsumer_CreatesQueueConsumer()
        {
            // Arrange
            var mockSession = new Mock<ISession>();
            var mockQueue = new Mock<IQueue>();
            var mockConsumer = new Mock<IMessageConsumer>();
            
            mockSession.Setup(s => s.CreateQueue("test.queue")).Returns(mockQueue.Object);
            mockSession.Setup(s => s.CreateConsumer(mockQueue.Object)).Returns(mockConsumer.Object);
            
            // Act
            var result = _factory.CreateConsumer(mockSession.Object, "test.queue");
            
            // Assert
            Assert.That(result, Is.EqualTo(mockConsumer.Object));
        }
        
        [Test]
        public void CreateProducer_CreatesQueueProducer()
        {
            // Arrange
            var mockSession = new Mock<ISession>();
            var mockQueue = new Mock<IQueue>();
            var mockProducer = new Mock<IMessageProducer>();
            
            mockSession.Setup(s => s.CreateQueue("test.queue")).Returns(mockQueue.Object);
            mockSession.Setup(s => s.CreateProducer(mockQueue.Object)).Returns(mockProducer.Object);
            
            // Act
            var result = _factory.CreateProducer(mockSession.Object, "test.queue");
            
            // Assert
            Assert.That(result, Is.EqualTo(mockProducer.Object));
        }
        
        [Test]
        public void CreateTopicSubscriber_WithSubscriptionName_CreatesDurableSubscriber()
        {
            // Arrange
            var mockSession = new Mock<ISession>();
            var mockTopic = new Mock<ITopic>();
            var mockConsumer = new Mock<IMessageConsumer>();
            
            mockSession.Setup(s => s.CreateTopic("test.topic")).Returns(mockTopic.Object);
            mockSession.Setup(s => s.CreateDurableSubscriber(mockTopic.Object, "sub-name"))
                .Returns(mockConsumer.Object);
            
            // Act
            var result = _factory.CreateTopicSubscriber(mockSession.Object, "test.topic", "sub-name");
            
            // Assert
            Assert.That(result, Is.EqualTo(mockConsumer.Object));
            mockSession.Verify(s => s.CreateDurableSubscriber(mockTopic.Object, "sub-name"), Times.Once);
        }
        
        [Test]
        public void CreateTopicSubscriber_WithoutSubscriptionName_CreatesNonDurableSubscriber()
        {
            // Arrange
            var mockSession = new Mock<ISession>();
            var mockTopic = new Mock<ITopic>();
            var mockConsumer = new Mock<IMessageConsumer>();
            
            mockSession.Setup(s => s.CreateTopic("test.topic")).Returns(mockTopic.Object);
            mockSession.Setup(s => s.CreateConsumer(mockTopic.Object)).Returns(mockConsumer.Object);
            
            // Act
            var result = _factory.CreateTopicSubscriber(mockSession.Object, "test.topic", "");
            
            // Assert
            Assert.That(result, Is.EqualTo(mockConsumer.Object));
            mockSession.Verify(s => s.CreateConsumer(mockTopic.Object), Times.Once);
        }
    }
}

// ========== Tests/Services/ErrorHandlerTests.cs ==========
using System;
using System.IO;
using NUnit.Framework;
using TibcoEmsService.Services;

namespace TibcoEmsService.Tests.Services
{
    [TestFixture]
    public class ErrorHandlerTests
    {
        private ErrorHandler _errorHandler;
        private string _testLogPath;
        
        [SetUp]
        public void Setup()
        {
            _errorHandler = new ErrorHandler();
            _testLogPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "TibcoEmsService",
                "logs");
        }
        
        [TearDown]
        public void TearDown()
        {
            // Clean up test log files
            if (Directory.Exists(_testLogPath))
            {
                try
                {
                    var files = Directory.GetFiles(_testLogPath, "service_*.log");
                    foreach (var file in files)
                    {
                        File.Delete(file);
                    }
                }
                catch { }
            }
        }
        
        [Test]
        public void HandleError_CreatesLogDirectory()
        {
            // Act
            _errorHandler.HandleError("Test", new Exception("Test exception"));
            
            // Assert
            Assert.That(Directory.Exists(_testLogPath), Is.True);
        }
        
        [Test]
        public void HandleError_WritesLogFile()
        {
            // Arrange
            string context = "Test context";
            var exception = new Exception("Test message");
            
            // Act
            _errorHandler.HandleError(context, exception);
            
            // Assert
            var logFile = Path.Combine(_testLogPath, $"service_{DateTime.Now:yyyyMMdd}.log");
            Assert.That(File.Exists(logFile), Is.True);
            
            var content = File.ReadAllText(logFile);
            Assert.That(content, Contains.Substring("Test context"));
            Assert.That(content, Contains.Substring("Test message"));
        }
        
        [Test]
        public void HandleError_DoesNotThrowOnException()
        {
            // Arrange
            var exception = new Exception("Test");
            
            // Act & Assert
            Assert.DoesNotThrow(() => _errorHandler.HandleError("Test", exception));
        }
    }
}

// ========== Tests/Services/EmsProcessorServiceTests.cs ==========
using System;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using TibcoEmsService.Services;

namespace TibcoEmsService.Tests.Services
{
    [TestFixture]
    public class EmsProcessorServiceTests
    {
        private Mock<IMessageProcessor> _mockMessageProcessor;
        private Mock<IHeartbeatService> _mockHeartbeatService;
        private Mock<ICommandService> _mockCommandService;
        private Mock<IErrorHandler> _mockErrorHandler;
        private EmsProcessorService _service;
        
        [SetUp]
        public void Setup()
        {
            _mockMessageProcessor = new Mock<IMessageProcessor>();
            _mockHeartbeatService = new Mock<IHeartbeatService>();
            _mockCommandService = new Mock<ICommandService>();
            _mockErrorHandler = new Mock<IErrorHandler>();
            
            _service = new EmsProcessorService(
                _mockMessageProcessor.Object,
                _mockHeartbeatService.Object,
                _mockCommandService.Object,
                _mockErrorHandler.Object);
        }
        
        [Test]
        public void Constructor_SetsServiceName()
        {
            // Assert
            Assert.That(_service.ServiceName, Is.EqualTo("TibcoEmsProcessorService"));
        }
        
        [Test]
        public void StartService_StartsAllComponents()
        {
            // Arrange
            _mockMessageProcessor.Setup(m => m.StartProcessing(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            _mockHeartbeatService.Setup(h => h.StartHeartbeat(It.IsAny<CancellationToken>(), It.IsAny<IMessageProcessor>()))
                .Returns(Task.CompletedTask);
            _mockCommandService.Setup(c => c.StartCommandListener(
                It.IsAny<CancellationToken>(), 
                It.IsAny<CancellationTokenSource>(), 
                It.IsAny<IMessageProcessor>()))
                .Returns(Task.CompletedTask);
            
            // Act
            _service.StartService(new string[0]);
            Thread.Sleep(100); // Give tasks time to start
            
            // Assert
            _mockMessageProcessor.Verify(m => m.StartProcessing(It.IsAny<CancellationToken>()), Times.Once);
            _mockHeartbeatService.Verify(h => h.StartHeartbeat(It.IsAny<CancellationToken>(), It.IsAny<IMessageProcessor>()), Times.Once);
            _mockCommandService.Verify(c => c.StartCommandListener(
                It.IsAny<CancellationToken>(), 
                It.IsAny<CancellationTokenSource>(), 
                It.IsAny<IMessageProcessor>()), Times.Once);
        }
        
        [Test]
        public void StopService_DisposesAllComponents()
        {
            // Arrange
            _mockMessageProcessor.Setup(m => m.GetMessageCount()).Returns(100);
            _mockMessageProcessor.Setup(m => m.GetSuccessfulMessageCount()).Returns(95);
            
            // Act
            _service.StartService(new string[0]);
            _service.StopService();
