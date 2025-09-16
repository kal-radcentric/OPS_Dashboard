using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Input;
using CommunityToolkit.Mvvm.Input;
using OPS_Dashboard.Services;

namespace OPS_Dashboard
{
    public class MainViewModel : INotifyPropertyChanged
    {
        private readonly FtpDownloadService _ftpService;
        private readonly CsvFileProcessorService _processorService;
        private CancellationTokenSource? _downloadCts;
        private CancellationTokenSource? _processingCts;

        // Download properties
        private string _downloadStatus = "Ready";
        private int _downloadProgress;
        private string _currentDownloadFile = "";
        private string _downloadLog = "";
        private bool _isDownloading;
        private bool _isDownloadEnabled = true;

        // Processing properties
        private string _processingStatus = "Ready";
        private int _processingProgress;
        private string _currentProcessingFile = "";
        private string _processingLog = "";
        private bool _isProcessing;
        private bool _isProcessingEnabled = true;

        // General properties
        private string _statusMessage = "Application ready";
        private DateTime _lastUpdateTime = DateTime.Now;

        public MainViewModel()
        {
            _ftpService = new FtpDownloadService();
            _processorService = new CsvFileProcessorService();

            // Subscribe to service events
            _ftpService.ProgressChanged += OnFtpProgressChanged;
            _ftpService.LogMessage += OnFtpLogMessage;

            _processorService.ProgressChanged += OnProcessingProgressChanged;
            _processorService.LogMessage += OnProcessingLogMessage;

            // Initialize commands
            DownloadFilesCommand = new AsyncRelayCommand(DownloadFilesAsync);
            CancelDownloadCommand = new RelayCommand(CancelDownload);
            ProcessFilesCommand = new AsyncRelayCommand(ProcessFilesAsync);
            CancelProcessingCommand = new RelayCommand(CancelProcessing);
        }

        // Commands
        public ICommand DownloadFilesCommand { get; }
        public ICommand CancelDownloadCommand { get; }
        public ICommand ProcessFilesCommand { get; }
        public ICommand CancelProcessingCommand { get; }

        // Download Properties
        public string DownloadStatus
        {
            get => _downloadStatus;
            set
            {
                _downloadStatus = value;
                OnPropertyChanged();
            }
        }

        public int DownloadProgress
        {
            get => _downloadProgress;
            set
            {
                _downloadProgress = value;
                OnPropertyChanged();
            }
        }

        public string CurrentDownloadFile
        {
            get => _currentDownloadFile;
            set
            {
                _currentDownloadFile = value;
                OnPropertyChanged();
            }
        }

        public string DownloadLog
        {
            get => _downloadLog;
            set
            {
                _downloadLog = value;
                OnPropertyChanged();
            }
        }

        public bool IsDownloading
        {
            get => _isDownloading;
            set
            {
                _isDownloading = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(IsDownloadEnabled));
            }
        }

        public bool IsDownloadEnabled => !_isDownloading;

        // Processing Properties
        public string ProcessingStatus
        {
            get => _processingStatus;
            set
            {
                _processingStatus = value;
                OnPropertyChanged();
            }
        }

        public int ProcessingProgress
        {
            get => _processingProgress;
            set
            {
                _processingProgress = value;
                OnPropertyChanged();
            }
        }

        public string CurrentProcessingFile
        {
            get => _currentProcessingFile;
            set
            {
                _currentProcessingFile = value;
                OnPropertyChanged();
            }
        }

        public string ProcessingLog
        {
            get => _processingLog;
            set
            {
                _processingLog = value;
                OnPropertyChanged();
            }
        }

        public bool IsProcessing
        {
            get => _isProcessing;
            set
            {
                _isProcessing = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(IsProcessingEnabled));
            }
        }

        public bool IsProcessingEnabled => !_isProcessing && !_isDownloading;

        // General Properties
        public string StatusMessage
        {
            get => _statusMessage;
            set
            {
                _statusMessage = value;
                OnPropertyChanged();
            }
        }

        public DateTime LastUpdateTime
        {
            get => _lastUpdateTime;
            set
            {
                _lastUpdateTime = value;
                OnPropertyChanged();
            }
        }

        // Download Methods
        private async Task DownloadFilesAsync()
        {
            try
            {
                IsDownloading = true;
                DownloadStatus = "Downloading...";
                DownloadProgress = 0;
                DownloadLog = "";
                StatusMessage = "Downloading files from SFTP servers...";

                _downloadCts = new CancellationTokenSource();
                var success = await _ftpService.DownloadAllFilesAsync(_downloadCts.Token);

                if (success)
                {
                    DownloadStatus = "Complete";
                    StatusMessage = "All files downloaded successfully";
                    DownloadProgress = 100;
                }
                else
                {
                    DownloadStatus = _downloadCts.IsCancellationRequested ? "Cancelled" : "Failed";
                    StatusMessage = _downloadCts.IsCancellationRequested
                        ? "Download cancelled by user"
                        : "Download failed - check log for details";
                }
            }
            catch (Exception ex)
            {
                DownloadStatus = "Error";
                StatusMessage = $"Download error: {ex.Message}";
                AppendToDownloadLog($"ERROR: {ex.Message}");
            }
            finally
            {
                IsDownloading = false;
                LastUpdateTime = DateTime.Now;
                _downloadCts?.Dispose();
                _downloadCts = null;
            }
        }

        private void CancelDownload()
        {
            _downloadCts?.Cancel();
            StatusMessage = "Cancelling download...";
        }

        // Processing Methods
        private async Task ProcessFilesAsync()
        {
            try
            {
                IsProcessing = true;
                ProcessingStatus = "Processing...";
                ProcessingProgress = 0;
                ProcessingLog = "";
                StatusMessage = "Processing CSV files...";

                _processingCts = new CancellationTokenSource();
                var success = await _processorService.ProcessAllFilesAsync(_processingCts.Token);

                if (success)
                {
                    ProcessingStatus = "Complete";
                    StatusMessage = "All files processed successfully";
                    ProcessingProgress = 100;
                }
                else
                {
                    ProcessingStatus = _processingCts.IsCancellationRequested ? "Cancelled" : "Failed";
                    StatusMessage = _processingCts.IsCancellationRequested
                        ? "Processing cancelled by user"
                        : "Processing failed - check log for details";
                }
            }
            catch (Exception ex)
            {
                ProcessingStatus = "Error";
                StatusMessage = $"Processing error: {ex.Message}";
                AppendToProcessingLog($"ERROR: {ex.Message}");
            }
            finally
            {
                IsProcessing = false;
                LastUpdateTime = DateTime.Now;
                _processingCts?.Dispose();
                _processingCts = null;
            }
        }

        private void CancelProcessing()
        {
            _processingCts?.Cancel();
            StatusMessage = "Cancelling processing...";
        }

        // Event Handlers
        private void OnFtpProgressChanged(object? sender, DownloadProgressEventArgs e)
        {
            DownloadProgress = e.PercentComplete;
            CurrentDownloadFile = e.Message;
            LastUpdateTime = DateTime.Now;
        }

        private void OnFtpLogMessage(object? sender, string message)
        {
            AppendToDownloadLog(message);
        }

        private void OnProcessingProgressChanged(object? sender, ProcessingProgressEventArgs e)
        {
            ProcessingProgress = e.PercentComplete;
            CurrentProcessingFile = e.Message;
            LastUpdateTime = DateTime.Now;
        }

        private void OnProcessingLogMessage(object? sender, string message)
        {
            AppendToProcessingLog(message);
        }

        private void AppendToDownloadLog(string message)
        {
            DownloadLog += $"[{DateTime.Now:HH:mm:ss}] {message}\n";
        }

        private void AppendToProcessingLog(string message)
        {
            ProcessingLog += $"[{DateTime.Now:HH:mm:ss}] {message}\n";
        }

        // INotifyPropertyChanged Implementation
        public event PropertyChangedEventHandler? PropertyChanged;

        protected virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}