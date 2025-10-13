using System;
using System.IO;
using System.Threading;
using Renci.SshNet;

namespace MySftpLibrary
{
    public class SftpClientService : IDisposable
    {
        private readonly string _host;
        private readonly int _port;
        private readonly string _username;
        private readonly string _password;
        private SftpClient _sftpClient;

        private const int MaxRetries = 3;
        private const int RetryDelayMilliseconds = 2000; // 2 seconds delay between retries

        public SftpClientService(string host, int port, string username, string password)
        {
            _host = host;
            _port = port;
            _username = username;
            _password = password;
        }

        /// <summary>
        /// Connects to the SFTP server.
        /// </summary>
        public void Connect()
        {
            if (_sftpClient != null && _sftpClient.IsConnected)
                return;

            _sftpClient = new SftpClient(_host, _port, _username, _password);
            _sftpClient.Connect();
        }

        /// <summary>
        /// Uploads a file to the SFTP server with retry logic.
        /// </summary>
        public void UploadFile(string localFilePath, string remoteFilePath)
        {
            if (!File.Exists(localFilePath))
                throw new FileNotFoundException("Local file not found.", localFilePath);

            EnsureConnected();

            int attempt = 0;
            while (true)
            {
                try
                {
                    using (var fileStream = new FileStream(localFilePath, FileMode.Open))
                    {
                        _sftpClient.UploadFile(fileStream, remoteFilePath, true);
                    }

                    // Success, break out of retry loop
                    break;
                }
                catch (Exception ex)
                {
                    attempt++;
                    if (attempt >= MaxRetries)
                        throw new Exception($"Failed to upload file after {MaxRetries} attempts. Last error: {ex.Message}", ex);

                    Console.WriteLine($"[Upload Attempt {attempt}] Error: {ex.Message}. Retrying in {RetryDelayMilliseconds / 1000} seconds...");
                    Thread.Sleep(RetryDelayMilliseconds);

                    // Reconnect and retry
                    Reconnect();
                }
            }
        }

        /// <summary>
        /// Downloads a file from the SFTP server with retry logic.
        /// </summary>
        public void DownloadFile(string remoteFilePath, string localFilePath)
        {
            EnsureConnected();

            int attempt = 0;
            while (true)
            {
                try
                {
                    using (var fileStream = new FileStream(localFilePath, FileMode.Create))
                    {
                        _sftpClient.DownloadFile(remoteFilePath, fileStream);
                    }

                    // Success, break out of retry loop
                    break;
                }
                catch (Exception ex)
                {
                    attempt++;
                    if (attempt >= MaxRetries)
                        throw new Exception($"Failed to download file after {MaxRetries} attempts. Last error: {ex.Message}", ex);

                    Console.WriteLine($"[Download Attempt {attempt}] Error: {ex.Message}. Retrying in {RetryDelayMilliseconds / 1000} seconds...");
                    Thread.Sleep(RetryDelayMilliseconds);

                    // Reconnect and retry
                    Reconnect();
                }
            }
        }

        /// <summary>
        /// Ensures the SFTP connection is active.
        /// </summary>
        private void EnsureConnected()
        {
            if (_sftpClient == null || !_sftpClient.IsConnected)
            {
                Connect();
            }
        }

        /// <summary>
        /// Forces reconnection to the SFTP server.
        /// </summary>
        private void Reconnect()
        {
            try
            {
                Disconnect();
            }
            catch { /* ignore */ }

            Connect();
        }

        /// <summary>
        /// Disconnects from the SFTP server.
        /// </summary>
        public void Disconnect()
        {
            if (_sftpClient != null && _sftpClient.IsConnected)
            {
                _sftpClient.Disconnect();
            }
        }

        public void Dispose()
        {
            Disconnect();
            _sftpClient?.Dispose();
        }
    }
}
