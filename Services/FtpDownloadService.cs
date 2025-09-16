using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Renci.SshNet;
using Renci.SshNet.Sftp;

namespace OPS_Dashboard.Services
{
    public class FtpConnection
    {
        public string Name { get; set; } = string.Empty;
        public string Protocol { get; set; } = string.Empty;
        public string Host { get; set; } = string.Empty;
        public string User { get; set; } = string.Empty;
        public string Password { get; set; } = string.Empty;
        public string RemoteFolder { get; set; } = string.Empty;
        public string BuId { get; set; } = string.Empty;
    }

    public class DownloadProgressEventArgs : EventArgs
    {
        public string FileName { get; set; } = string.Empty;
        public long BytesDownloaded { get; set; }
        public long TotalBytes { get; set; }
        public int PercentComplete { get; set; }
        public string Message { get; set; } = string.Empty;
    }

    public class FtpDownloadService
    {
        private readonly string _localDirectory = @"C:\sfmc_ftp";
        private readonly string _ftpConnectionsFile = "ftp_connections.txt";
        private readonly string _fileListFile = "FileList.txt";
        private CancellationTokenSource? _cancellationTokenSource;

        public event EventHandler<DownloadProgressEventArgs>? ProgressChanged;
        public event EventHandler<string>? LogMessage;

        public FtpDownloadService()
        {
            if (!Directory.Exists(_localDirectory))
            {
                Directory.CreateDirectory(_localDirectory);
            }
        }

        public async Task<bool> DownloadAllFilesAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                LogMessage?.Invoke(this, $"Starting download process at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                LogMessage?.Invoke(this, $"Local directory: {_localDirectory}");

                // Step 1: Delete existing CSV files
                LogMessage?.Invoke(this, "\n=== STEP 1: CLEANING LOCAL DIRECTORY ===");
                await DeleteExistingCsvFiles(cancellationToken);

                // Step 2: Download new files
                LogMessage?.Invoke(this, "\n=== STEP 2: DOWNLOADING FILES ===");

                var connections = ParseFtpConnections();
                var requiredFiles = GetRequiredFiles();

                if (connections.Count == 0)
                {
                    LogMessage?.Invoke(this, "No FTP connections found in configuration file.");
                    return false;
                }

                LogMessage?.Invoke(this, $"Found {connections.Count} FTP connections");
                LogMessage?.Invoke(this, $"Need to download {requiredFiles.Count} files total");

                int totalFiles = requiredFiles.Count;
                int currentFileIndex = 0;

                foreach (var connection in connections)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        LogMessage?.Invoke(this, "Download cancelled by user");
                        return false;
                    }

                    var filesForThisBu = requiredFiles.Where(f => f.Contains($"_{connection.BuId}_")).ToList();
                    if (filesForThisBu.Count == 0)
                    {
                        LogMessage?.Invoke(this, $"No files to download for {connection.Name}");
                        continue;
                    }

                    LogMessage?.Invoke(this, $"\nConnecting to {connection.Name} ({connection.Host})...");

                    try
                    {
                        using (var client = new SftpClient(connection.Host, connection.User, connection.Password))
                        {
                            client.Connect();
                            LogMessage?.Invoke(this, $"Connected successfully to {connection.Name}");

                            client.ChangeDirectory(connection.RemoteFolder);
                            LogMessage?.Invoke(this, $"Changed to remote directory: {connection.RemoteFolder}");

                            var remoteFiles = client.ListDirectory(connection.RemoteFolder).ToList();
                            LogMessage?.Invoke(this, $"Found {remoteFiles.Count} files in remote directory");

                            foreach (var requiredFile in filesForThisBu)
                            {
                                if (cancellationToken.IsCancellationRequested) break;

                                currentFileIndex++;
                                var fileName = Path.GetFileName(requiredFile);
                                var remoteFile = remoteFiles.FirstOrDefault(f => f.Name == fileName);

                                if (remoteFile == null)
                                {
                                    LogMessage?.Invoke(this, $"File not found on server: {fileName}");
                                    continue;
                                }

                                var localPath = Path.Combine(_localDirectory, fileName);
                                LogMessage?.Invoke(this, $"Downloading: {fileName} ({FormatBytes(remoteFile.Length)})");

                                await DownloadFileAsync(client, remoteFile, localPath, currentFileIndex, totalFiles, cancellationToken);

                                if (cancellationToken.IsCancellationRequested) break;
                            }

                            client.Disconnect();
                        }
                    }
                    catch (Exception ex)
                    {
                        LogMessage?.Invoke(this, $"Error connecting to {connection.Name}: {ex.Message}");
                    }
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    LogMessage?.Invoke(this, $"\nDownload completed successfully at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogMessage?.Invoke(this, $"Fatal error during download: {ex.Message}");
                return false;
            }
        }

        private async Task DeleteExistingCsvFiles(CancellationToken cancellationToken)
        {
            try
            {
                LogMessage?.Invoke(this, $"Scanning for existing CSV files in: {_localDirectory}");

                // Get all CSV files in the directory
                var csvFiles = Directory.GetFiles(_localDirectory, "*.csv");

                if (csvFiles.Length == 0)
                {
                    LogMessage?.Invoke(this, "No existing CSV files found to delete.");
                    ProgressChanged?.Invoke(this, new DownloadProgressEventArgs
                    {
                        FileName = "Directory Cleanup",
                        PercentComplete = 5,
                        Message = "Directory cleanup completed - no files to delete"
                    });
                    return;
                }

                LogMessage?.Invoke(this, $"Found {csvFiles.Length} CSV files to delete");

                int deletedCount = 0;
                int failedCount = 0;

                foreach (var filePath in csvFiles)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        LogMessage?.Invoke(this, "File deletion cancelled by user");
                        return;
                    }

                    var fileName = Path.GetFileName(filePath);
                    try
                    {
                        File.Delete(filePath);
                        deletedCount++;
                        LogMessage?.Invoke(this, $"Deleted: {fileName}");

                        // Report progress (deletion takes up to 5% of total progress)
                        var progress = (deletedCount * 5) / csvFiles.Length;
                        ProgressChanged?.Invoke(this, new DownloadProgressEventArgs
                        {
                            FileName = fileName,
                            PercentComplete = progress,
                            Message = $"Deleting files: {deletedCount}/{csvFiles.Length} - {fileName}"
                        });

                        // Small delay to show progress
                        await Task.Delay(50, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        failedCount++;
                        LogMessage?.Invoke(this, $"Failed to delete {fileName}: {ex.Message}");
                    }
                }

                LogMessage?.Invoke(this, $"Directory cleanup completed: {deletedCount} files deleted, {failedCount} failed");

                // Final progress update for cleanup phase
                ProgressChanged?.Invoke(this, new DownloadProgressEventArgs
                {
                    FileName = "Directory Cleanup",
                    PercentComplete = 5,
                    Message = $"Directory cleanup completed - {deletedCount} files deleted"
                });
            }
            catch (Exception ex)
            {
                LogMessage?.Invoke(this, $"Error during file cleanup: {ex.Message}");
            }
        }

        private async Task DownloadFileAsync(SftpClient client, ISftpFile remoteFile, string localPath,
            int currentIndex, int totalFiles, CancellationToken cancellationToken)
        {
            try
            {
                using (var fileStream = File.Create(localPath))
                {
                    var asyncResult = client.BeginDownloadFile(remoteFile.FullName, fileStream);
                    var downloadTask = Task.Factory.FromAsync(asyncResult, client.EndDownloadFile);

                    long lastReportedBytes = 0;
                    var lastReportTime = DateTime.Now;

                    while (!downloadTask.IsCompleted && !cancellationToken.IsCancellationRequested)
                    {
                        await Task.Delay(100);

                        var currentBytes = fileStream.Position;
                        var percentComplete = (int)((currentBytes * 100) / remoteFile.Length);
                        // Adjust for 5% used by file deletion - downloads use 5-100% of progress
                        var downloadProgress = ((currentIndex - 1) * 95 + (percentComplete * 95 / 100)) / totalFiles;
                        var overallPercent = 5 + downloadProgress;

                        if (currentBytes != lastReportedBytes || (DateTime.Now - lastReportTime).TotalSeconds > 1)
                        {
                            var progress = new DownloadProgressEventArgs
                            {
                                FileName = remoteFile.Name,
                                BytesDownloaded = currentBytes,
                                TotalBytes = remoteFile.Length,
                                PercentComplete = overallPercent,
                                Message = $"File {currentIndex}/{totalFiles}: {remoteFile.Name} - {FormatBytes(currentBytes)}/{FormatBytes(remoteFile.Length)} ({percentComplete}%)"
                            };

                            ProgressChanged?.Invoke(this, progress);
                            lastReportedBytes = currentBytes;
                            lastReportTime = DateTime.Now;
                        }
                    }

                    if (cancellationToken.IsCancellationRequested)
                    {
                        fileStream.Close();
                        if (File.Exists(localPath))
                        {
                            File.Delete(localPath);
                        }
                    }
                    else
                    {
                        await downloadTask;
                        LogMessage?.Invoke(this, $"Downloaded: {remoteFile.Name} - {FormatBytes(remoteFile.Length)}");
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage?.Invoke(this, $"Error downloading {remoteFile.Name}: {ex.Message}");
                if (File.Exists(localPath))
                {
                    File.Delete(localPath);
                }
            }
        }

        private List<FtpConnection> ParseFtpConnections()
        {
            var connections = new List<FtpConnection>();

            if (!File.Exists(_ftpConnectionsFile))
            {
                LogMessage?.Invoke(this, $"FTP connections file not found: {_ftpConnectionsFile}");
                return connections;
            }

            var lines = File.ReadAllLines(_ftpConnectionsFile);
            FtpConnection? currentConnection = null;

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                if (line.StartsWith("[") && line.EndsWith("]"))
                {
                    if (currentConnection != null)
                    {
                        connections.Add(currentConnection);
                    }

                    var name = line.Trim('[', ']');
                    currentConnection = new FtpConnection { Name = name };

                    // Extract BU ID from name (e.g., "bu2532 - HCP - Brenso DSA" -> "2532")
                    var parts = name.Split('-');
                    if (parts.Length > 0 && parts[0].Trim().StartsWith("bu"))
                    {
                        currentConnection.BuId = parts[0].Trim().Substring(2);
                    }
                }
                else if (currentConnection != null && line.Contains("="))
                {
                    var parts = line.Split('=', 2);
                    if (parts.Length == 2)
                    {
                        var key = parts[0].Trim();
                        var value = parts[1].Trim();

                        switch (key.ToLower())
                        {
                            case "protocol":
                                currentConnection.Protocol = value;
                                break;
                            case "host":
                                currentConnection.Host = value;
                                break;
                            case "user":
                                currentConnection.User = value;
                                break;
                            case "pwd":
                                currentConnection.Password = value;
                                break;
                            case "remotefolder":
                                currentConnection.RemoteFolder = value;
                                break;
                        }
                    }
                }
            }

            if (currentConnection != null)
            {
                connections.Add(currentConnection);
            }

            return connections;
        }

        private List<string> GetRequiredFiles()
        {
            var files = new List<string>();

            if (!File.Exists(_fileListFile))
            {
                LogMessage?.Invoke(this, $"File list not found: {_fileListFile}");
                return files;
            }

            var lines = File.ReadAllLines(_fileListFile);
            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line) || line.TrimStart().StartsWith("#"))
                    continue;

                var parts = line.Split('|');
                if (parts.Length == 2)
                {
                    files.Add(parts[1].Trim());
                }
            }

            return files.Select(Path.GetFileName).Distinct().ToList();
        }

        private string FormatBytes(long bytes)
        {
            string[] sizes = { "B", "KB", "MB", "GB" };
            double len = bytes;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len = len / 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }
    }
}