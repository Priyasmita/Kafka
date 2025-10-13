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
