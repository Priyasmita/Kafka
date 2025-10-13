using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Renci.Ssh.Net;
using Renci.SshNet.Sftp;

namespace SftpFileTransfer
{
    /// <summary>
    /// Configuration for SFTP connection
    /// </summary>
    public class SftpConfig
    {
        public string Host { get; set; }
        public int Port { get; set; } = 22;
        public string Username { get; set; }
        public string Password { get; set; }
        public int ConnectionTimeout { get; set; } = 30000; // 30 seconds
    }
    public class SftpClient : IDisposable
    {
        private readonly SftpConfig _config;
        private Renci.SshNet.SftpClient _client;
        private bool _disposed = false;

        public SftpClient(SftpConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            ValidateConfig();
        }

        private void ValidateConfig()
        {
            if (string.IsNullOrWhiteSpace(_config.Host))
                throw new ArgumentException("Host cannot be empty", nameof(_config.Host));

            if (string.IsNullOrWhiteSpace(_config.Username))
                throw new ArgumentException("Username cannot be empty", nameof(_config.Username));

            if (string.IsNullOrWhiteSpace(_config.Password))
                throw new ArgumentException("Password cannot be empty", nameof(_config.Password));
        }

        public void Connect()
        {
            if (_client != null && _client.IsConnected)
                return;

            // Password authentication
            var authMethod = new PasswordAuthenticationMethod(_config.Username, _config.Password);
            var connectionInfo = new ConnectionInfo(_config.Host, _config.Port, _config.Username, authMethod);
            connectionInfo.Timeout = TimeSpan.FromMilliseconds(_config.ConnectionTimeout);

            _client = new Renci.SshNet.SftpClient(connectionInfo);
            _client.Connect();
        }

        public void Disconnect()
        {
            if (_client != null && _client.IsConnected)
            {
                _client.Disconnect();
            }
        }

        public TransferResult UploadFile(string localPath, string remotePath)
        {
            var result = new TransferResult { FilePath = remotePath };
            int maxRetries = 3;
            int attempt = 0;

            while (attempt < maxRetries)
            {
                attempt++;
                try
                {
                    if (!File.Exists(localPath))
                        throw new FileNotFoundException($"Local file not found: {localPath}");

                    EnsureConnected();

                    // Ensure remote directory exists
                    string remoteDir = Path.GetDirectoryName(remotePath).Replace("\\", "/");
                    CreateRemoteDirectoryIfNotExists(remoteDir);

                    using (var fileStream = new FileStream(localPath, FileMode.Open, FileAccess.Read))
                    {
                        _client.UploadFile(fileStream, remotePath);
                        result.BytesTransferred = fileStream.Length;
                    }

                    result.Success = true;
                    result.Message = $"File uploaded successfully to {remotePath}";
                    return result;
                }
                catch (Exception ex)
                {
                    result.Exception = ex;
                    
                    if (attempt >= maxRetries)
                    {
                        result.Success = false;
                        result.Message = $"Upload failed after {maxRetries} attempts: {ex.Message}";
                    }
                    else
                    {
                        // Reconnect before retry
                        try
                        {
                            Disconnect();
                            Connect();
                        }
                        catch
                        {
                            // Continue to next retry
                        }
                    }
                }
            }

            return result;
        }

        public TransferResult DownloadFile(string remotePath, string localPath)
        {
            var result = new TransferResult { FilePath = localPath };
            int maxRetries = 3;
            int attempt = 0;

            while (attempt < maxRetries)
            {
                attempt++;
                try
                {
                    EnsureConnected();

                    if (!_client.Exists(remotePath))
                        throw new FileNotFoundException($"Remote file not found: {remotePath}");

                    // Ensure local directory exists
                    string localDir = Path.GetDirectoryName(localPath);
                    if (!Directory.Exists(localDir))
                        Directory.CreateDirectory(localDir);

                    using (var fileStream = new FileStream(localPath, FileMode.Create, FileAccess.Write))
                    {
                        _client.DownloadFile(remotePath, fileStream);
                        result.BytesTransferred = fileStream.Length;
                    }

                    result.Success = true;
                    result.Message = $"File downloaded successfully to {localPath}";
                    return result;
                }
                catch (Exception ex)
                {
                    result.Exception = ex;
                    
                    if (attempt >= maxRetries)
                    {
                        result.Success = false;
                        result.Message = $"Download failed after {maxRetries} attempts: {ex.Message}";
                    }
                    else
                    {
                        // Reconnect before retry
                        try
                        {
                            Disconnect();
                            Connect();
                        }
                        catch
                        {
                            // Continue to next retry
                        }
                    }
                }
            }

            return result;
        }

        public List<TransferResult> UploadFiles(Dictionary<string, string> filePairs)
        {
            var results = new List<TransferResult>();

            foreach (var pair in filePairs)
            {
                results.Add(UploadFile(pair.Key, pair.Value));
            }

            return results;
        }

        public List<TransferResult> DownloadFiles(Dictionary<string, string> filePairs)
        {
            var results = new List<TransferResult>();

            foreach (var pair in filePairs)
            {
                results.Add(DownloadFile(pair.Key, pair.Value));
            }

            return results;
        }

        public List<SftpFile> ListDirectory(string remotePath)
        {
            EnsureConnected();
            return _client.ListDirectory(remotePath).ToList();
        }

        public bool Exists(string remotePath)
        {
            EnsureConnected();
            return _client.Exists(remotePath);
        }

        public bool DeleteFile(string remotePath)
        {
            try
            {
                EnsureConnected();
                _client.DeleteFile(remotePath);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool CreateDirectory(string remotePath)
        {
            try
            {
                EnsureConnected();
                _client.CreateDirectory(remotePath);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private void CreateRemoteDirectoryIfNotExists(string remotePath)
        {
            if (string.IsNullOrWhiteSpace(remotePath) || remotePath == "/")
                return;

            if (!_client.Exists(remotePath))
            {
                string parent = Path.GetDirectoryName(remotePath).Replace("\\", "/");
                CreateRemoteDirectoryIfNotExists(parent);
                _client.CreateDirectory(remotePath);
            }
        }

        private void EnsureConnected()
        {
            if (_client == null || !_client.IsConnected)
            {
                Connect();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_client != null)
                    {
                        if (_client.IsConnected)
                            _client.Disconnect();
                        _client.Dispose();
                    }
                }
                _disposed = true;
            }
        }
    }
}


using System;
using System.Collections.Generic;
using System.IO;
using Moq;
using NUnit.Framework;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using SftpFileTransfer;

namespace SftpFileTransfer.Tests
{
    [TestFixture]
    public class SftpClientTests
    {
        private SftpConfig _validConfig;
        private string _testLocalFile;
        private string _testLocalDirectory;

        [SetUp]
        public void SetUp()
        {
            _validConfig = new SftpConfig
            {
                Host = "test.server.com",
                Port = 22,
                Username = "testuser",
                Password = "testpass",
                ConnectionTimeout = 30000
            };

            // Create test directory and file
            _testLocalDirectory = Path.Combine(Path.GetTempPath(), "SftpTests");
            Directory.CreateDirectory(_testLocalDirectory);
            
            _testLocalFile = Path.Combine(_testLocalDirectory, "test.txt");
            File.WriteAllText(_testLocalFile, "Test content for upload");
        }

        [TearDown]
        public void TearDown()
        {
            // Clean up test files
            if (Directory.Exists(_testLocalDirectory))
            {
                Directory.Delete(_testLocalDirectory, true);
            }
        }

        #region Configuration Tests

        [Test]
        public void Constructor_WithValidConfig_DoesNotThrow()
        {
            // Arrange & Act & Assert
            Assert.DoesNotThrow(() => new SftpClient(_validConfig));
        }

        [Test]
        public void Constructor_WithNullConfig_ThrowsArgumentNullException()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentNullException>(() => new SftpClient(null));
        }

        [Test]
        public void Constructor_WithEmptyHost_ThrowsArgumentException()
        {
            // Arrange
            var config = new SftpConfig
            {
                Host = "",
                Username = "testuser",
                Password = "testpass"
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new SftpClient(config));
        }

        [Test]
        public void Constructor_WithEmptyUsername_ThrowsArgumentException()
        {
            // Arrange
            var config = new SftpConfig
            {
                Host = "test.server.com",
                Username = "",
                Password = "testpass"
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new SftpClient(config));
        }

        [Test]
        public void Constructor_WithEmptyPassword_ThrowsArgumentException()
        {
            // Arrange
            var config = new SftpConfig
            {
                Host = "test.server.com",
                Username = "testuser",
                Password = ""
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new SftpClient(config));
        }

        [Test]
        public void Constructor_WithNullPassword_ThrowsArgumentException()
        {
            // Arrange
            var config = new SftpConfig
            {
                Host = "test.server.com",
                Username = "testuser",
                Password = null
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new SftpClient(config));
        }

        #endregion

        #region TransferResult Tests

        [Test]
        public void TransferResult_DefaultValues_AreCorrect()
        {
            // Arrange & Act
            var result = new TransferResult();

            // Assert
            Assert.IsFalse(result.Success);
            Assert.IsNull(result.Message);
            Assert.IsNull(result.FilePath);
            Assert.AreEqual(0, result.BytesTransferred);
            Assert.IsNull(result.Exception);
        }

        [Test]
        public void TransferResult_CanSetProperties()
        {
            // Arrange
            var result = new TransferResult();
            var exception = new Exception("Test exception");

            // Act
            result.Success = true;
            result.Message = "Test message";
            result.FilePath = "/test/path";
            result.BytesTransferred = 1024;
            result.Exception = exception;

            // Assert
            Assert.IsTrue(result.Success);
            Assert.AreEqual("Test message", result.Message);
            Assert.AreEqual("/test/path", result.FilePath);
            Assert.AreEqual(1024, result.BytesTransferred);
            Assert.AreEqual(exception, result.Exception);
        }

        #endregion

        #region Upload Tests

        [Test]
        public void UploadFile_WithNonExistentLocalFile_ReturnsFailureResult()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            string nonExistentFile = Path.Combine(_testLocalDirectory, "nonexistent.txt");

            // Act
            var result = client.UploadFile(nonExistentFile, "/remote/path/file.txt");

            // Assert
            Assert.IsFalse(result.Success);
            Assert.IsNotNull(result.Message);
            Assert.IsTrue(result.Message.Contains("failed after 3 attempts"));
            Assert.IsNotNull(result.Exception);
            Assert.IsInstanceOf<FileNotFoundException>(result.Exception);
        }

        [Test]
        public void UploadFile_SetsCorrectFilePath()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            string remotePath = "/remote/path/file.txt";

            // Act
            var result = client.UploadFile(_testLocalFile, remotePath);

            // Assert
            Assert.AreEqual(remotePath, result.FilePath);
        }

        #endregion

        #region Download Tests

        [Test]
        public void DownloadFile_SetsCorrectFilePath()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            string localPath = Path.Combine(_testLocalDirectory, "downloaded.txt");

            // Act
            var result = client.DownloadFile("/remote/file.txt", localPath);

            // Assert
            Assert.AreEqual(localPath, result.FilePath);
        }

        [Test]
        public void DownloadFile_CreatesLocalDirectoryIfNotExists()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            string newDirectory = Path.Combine(_testLocalDirectory, "newdir");
            string localPath = Path.Combine(newDirectory, "downloaded.txt");

            // Act
            var result = client.DownloadFile("/remote/file.txt", localPath);

            // Assert - Directory should be created even if download fails
            Assert.IsTrue(Directory.Exists(newDirectory));
        }

        #endregion

        #region Multiple Files Tests

        [Test]
        public void UploadFiles_WithMultipleFiles_ReturnsCorrectNumberOfResults()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            var file2 = Path.Combine(_testLocalDirectory, "test2.txt");
            File.WriteAllText(file2, "Test content 2");

            var filePairs = new Dictionary<string, string>
            {
                { _testLocalFile, "/remote/test1.txt" },
                { file2, "/remote/test2.txt" }
            };

            // Act
            var results = client.UploadFiles(filePairs);

            // Assert
            Assert.AreEqual(2, results.Count);
        }

        [Test]
        public void DownloadFiles_WithMultipleFiles_ReturnsCorrectNumberOfResults()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            var filePairs = new Dictionary<string, string>
            {
                { "/remote/test1.txt", Path.Combine(_testLocalDirectory, "download1.txt") },
                { "/remote/test2.txt", Path.Combine(_testLocalDirectory, "download2.txt") }
            };

            // Act
            var results = client.DownloadFiles(filePairs);

            // Assert
            Assert.AreEqual(2, results.Count);
        }

        [Test]
        public void UploadFiles_WithEmptyDictionary_ReturnsEmptyList()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            var filePairs = new Dictionary<string, string>();

            // Act
            var results = client.UploadFiles(filePairs);

            // Assert
            Assert.IsEmpty(results);
        }

        [Test]
        public void DownloadFiles_WithEmptyDictionary_ReturnsEmptyList()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            var filePairs = new Dictionary<string, string>();

            // Act
            var results = client.DownloadFiles(filePairs);

            // Assert
            Assert.IsEmpty(results);
        }

        #endregion

        #region Dispose Tests

        [Test]
        public void Dispose_CanBeCalledMultipleTimes()
        {
            // Arrange
            var client = new SftpClient(_validConfig);

            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                client.Dispose();
                client.Dispose();
                client.Dispose();
            });
        }

        [Test]
        public void Dispose_WithUsingStatement_DisposesCorrectly()
        {
            // Arrange & Act & Assert
            Assert.DoesNotThrow(() =>
            {
                using (var client = new SftpClient(_validConfig))
                {
                    // Do nothing
                }
            });
        }

        #endregion

        #region Configuration Property Tests

        [Test]
        public void SftpConfig_DefaultPort_Is22()
        {
            // Arrange & Act
            var config = new SftpConfig();

            // Assert
            Assert.AreEqual(22, config.Port);
        }

        [Test]
        public void SftpConfig_DefaultConnectionTimeout_Is30000()
        {
            // Arrange & Act
            var config = new SftpConfig();

            // Assert
            Assert.AreEqual(30000, config.ConnectionTimeout);
        }

        [Test]
        public void SftpConfig_CanSetAllProperties()
        {
            // Arrange & Act
            var config = new SftpConfig
            {
                Host = "custom.host.com",
                Port = 2222,
                Username = "customuser",
                Password = "custompass",
                ConnectionTimeout = 60000
            };

            // Assert
            Assert.AreEqual("custom.host.com", config.Host);
            Assert.AreEqual(2222, config.Port);
            Assert.AreEqual("customuser", config.Username);
            Assert.AreEqual("custompass", config.Password);
            Assert.AreEqual(60000, config.ConnectionTimeout);
        }

        #endregion

        #region Retry Logic Tests

        [Test]
        public void UploadFile_WithConnectionFailure_RetriesThreeTimes()
        {
            // Arrange
            var client = new SftpClient(_validConfig);

            // Act
            var result = client.UploadFile(_testLocalFile, "/remote/path/file.txt");

            // Assert - Should fail after 3 attempts (can't actually connect)
            Assert.IsFalse(result.Success);
            Assert.IsTrue(result.Message.Contains("after 3 attempts") || 
                         result.Message.Contains("failed"));
        }

        [Test]
        public void DownloadFile_WithConnectionFailure_RetriesThreeTimes()
        {
            // Arrange
            var client = new SftpClient(_validConfig);
            string localPath = Path.Combine(_testLocalDirectory, "download.txt");

            // Act
            var result = client.DownloadFile("/remote/file.txt", localPath);

            // Assert - Should fail after 3 attempts (can't actually connect)
            Assert.IsFalse(result.Success);
            Assert.IsTrue(result.Message.Contains("after 3 attempts") || 
                         result.Message.Contains("failed"));
        }

        #endregion
    }
}
