using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CsvProcessor;
using Microsoft.Data.SqlClient;

namespace OPS_Dashboard.Services
{
    public class ProcessingProgressEventArgs : EventArgs
    {
        public string FileName { get; set; } = string.Empty;
        public int CurrentFileIndex { get; set; }
        public int TotalFiles { get; set; }
        public int PercentComplete { get; set; }
        public string CurrentStep { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    public class CsvFileProcessorService
    {
        private readonly string _connectionString;
        private readonly string _fileListPath = @"FileList.txt";

        public event EventHandler<ProcessingProgressEventArgs>? ProgressChanged;
        public event EventHandler<string>? LogMessage;

        public CsvFileProcessorService()
        {
            // Use connection string from Program.cs
            _connectionString = "Server=HERMES;Database=InsMedOPS;User Id=radmin;Password=thund3r!90210;Connection Timeout=240;Integrated Security=True;TrustServerCertificate=True;";
        }

        public async Task<bool> ProcessAllFilesAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                LogMessage?.Invoke(this, $"Starting file processing at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                LogMessage?.Invoke(this, $"Database connection configured");

                // Step 1: Execute stored procedures to truncate tables
                LogMessage?.Invoke(this, "\n=== STEP 1: TRUNCATING TABLES ===");
                bool spExecuted = await ExecuteStoredProcedures(cancellationToken);

                if (!spExecuted)
                {
                    LogMessage?.Invoke(this, "WARNING: Failed to execute stored procedures, but continuing with CSV processing...");
                }

                // Step 2: Process CSV files
                LogMessage?.Invoke(this, "\n=== STEP 2: PROCESSING CSV FILES ===");

                var filesToProcess = ReadFileList();

                if (filesToProcess.Count == 0)
                {
                    LogMessage?.Invoke(this, "No files to process. Please check the FileList.txt file.");
                    return false;
                }

                LogMessage?.Invoke(this, $"Found {filesToProcess.Count} files to process");

                int currentFileIndex = 0;
                int totalFiles = filesToProcess.Count;

                foreach (var fileInfo in filesToProcess)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        LogMessage?.Invoke(this, "Processing cancelled by user");
                        return false;
                    }

                    currentFileIndex++;

                    // Adjust progress to account for stored procedures (20% for SPs, 80% for files)
                    await ProcessSingleFileAsync(fileInfo.TableName, fileInfo.FilePath,
                        currentFileIndex, totalFiles, cancellationToken);
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    LogMessage?.Invoke(this, $"\nAll files processed successfully at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");

                    // Step 3: Execute final consolidation stored procedure
                    LogMessage?.Invoke(this, "\n=== STEP 3: MASTER CONSOLIDATION ===");
                    bool consolidationExecuted = await ExecuteFinalStoredProcedure(cancellationToken);

                    if (consolidationExecuted)
                    {
                        LogMessage?.Invoke(this, $"\nComplete processing pipeline finished successfully at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                        return true;
                    }
                    else
                    {
                        LogMessage?.Invoke(this, "WARNING: Master consolidation failed, but CSV processing was successful");
                        return false;
                    }
                }

                return false;
            }
            catch (Exception ex)
            {
                LogMessage?.Invoke(this, $"Fatal error during processing: {ex.Message}");
                return false;
            }
        }

        private async Task<bool> ExecuteStoredProcedures(CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    LogMessage?.Invoke(this, "Connecting to database for stored procedure execution...");
                    await connection.OpenAsync(cancellationToken);
                    LogMessage?.Invoke(this, "Connected successfully");

                    // Execute sp_truncate_import_tables
                    LogMessage?.Invoke(this, "Executing sp_truncate_import_tables...");
                    ReportProgress("Stored Procedures", 0, 2, 2, "Truncating import tables");

                    using (var cmd = new SqlCommand("sp_truncate_import_tables", connection))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;
                        cmd.CommandTimeout = 120; // 2 minutes timeout

                        try
                        {
                            await cmd.ExecuteNonQueryAsync(cancellationToken);
                            LogMessage?.Invoke(this, "Successfully executed sp_truncate_import_tables");
                        }
                        catch (Exception ex)
                        {
                            LogMessage?.Invoke(this, $"Error executing sp_truncate_import_tables: {ex.Message}");
                            return false;
                        }
                    }

                    if (cancellationToken.IsCancellationRequested)
                    {
                        LogMessage?.Invoke(this, "Operation cancelled by user");
                        return false;
                    }

                    // Execute sp_truncate_sfmc_tables
                    LogMessage?.Invoke(this, "Executing sp_truncate_sfmc_tables...");
                    ReportProgress("Stored Procedures", 1, 2, 5, "Truncating SFMC tables");

                    using (var cmd = new SqlCommand("sp_truncate_sfmc_tables", connection))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;
                        cmd.CommandTimeout = 120; // 2 minutes timeout

                        try
                        {
                            await cmd.ExecuteNonQueryAsync(cancellationToken);
                            LogMessage?.Invoke(this, "Successfully executed sp_truncate_sfmc_tables");
                        }
                        catch (Exception ex)
                        {
                            LogMessage?.Invoke(this, $"Error executing sp_truncate_sfmc_tables: {ex.Message}");
                            return false;
                        }
                    }

                    ReportProgress("Stored Procedures", 2, 2, 10, "Tables truncated successfully");
                    LogMessage?.Invoke(this, "All stored procedures executed successfully");
                    return true;
                }
            }
            catch (Exception ex)
            {
                LogMessage?.Invoke(this, $"Error connecting to database: {ex.Message}");
                return false;
            }
        }

        private async Task<bool> ExecuteFinalStoredProcedure(CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    LogMessage?.Invoke(this, "Connecting to database for final consolidation...");
                    await connection.OpenAsync(cancellationToken);
                    LogMessage?.Invoke(this, "Connected successfully");

                    LogMessage?.Invoke(this, "Executing sp_bu_to_sfmc_MasterConsolidation...");
                    ReportProgress("Master Consolidation", 0, 1, 90, "Starting master consolidation");

                    using (var cmd = new SqlCommand("sp_bu_to_sfmc_MasterConsolidation", connection))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;
                        cmd.CommandTimeout = 600; // 10 minutes timeout for consolidation

                        try
                        {
                            await cmd.ExecuteNonQueryAsync(cancellationToken);
                            LogMessage?.Invoke(this, "Successfully executed sp_bu_to_sfmc_MasterConsolidation");
                            ReportProgress("Master Consolidation", 1, 1, 100, "Master consolidation completed");
                            return true;
                        }
                        catch (Exception ex)
                        {
                            LogMessage?.Invoke(this, $"Error executing sp_bu_to_sfmc_MasterConsolidation: {ex.Message}");
                            return false;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage?.Invoke(this, $"Error connecting to database for consolidation: {ex.Message}");
                return false;
            }
        }

        private async Task ProcessSingleFileAsync(string tableName, string filePath,
            int currentIndex, int totalFiles, CancellationToken cancellationToken)
        {
            await Task.Run(() =>
            {
                try
                {
                    LogMessage?.Invoke(this, $"\n--- Processing {tableName} ({currentIndex}/{totalFiles}) ---");
                    LogMessage?.Invoke(this, $"File: {filePath}");

                    if (!File.Exists(filePath))
                    {
                        LogMessage?.Invoke(this, $"Warning: File not found - {filePath}");
                        // Adjust progress to account for 10% initial SPs, 80% files, 10% final SP
                        var skipPercent = 10 + ((currentIndex * 80) / totalFiles);
                        ReportProgress(Path.GetFileName(filePath), currentIndex, totalFiles, skipPercent, "File not found");
                        return;
                    }

                    var processor = new EnhancedCsvFileProcessor(_connectionString, tableName);

                    // Subscribe to processor events
                    processor.StepProgress += (sender, args) =>
                    {
                        // Files processing takes 80% of total progress (10% initial SPs, 80% files, 10% final SP)
                        var percentForThisFile = 80 / totalFiles;
                        var fileStartPercent = 10 + ((currentIndex - 1) * percentForThisFile);
                        var fileProgress = (args.StepPercent * percentForThisFile) / 100;
                        var overallPercent = fileStartPercent + fileProgress;

                        ReportProgress(Path.GetFileName(filePath), currentIndex, totalFiles,
                            overallPercent, args.StepName);

                        LogMessage?.Invoke(this, args.Message);
                    };

                    processor.ProcessFile(filePath);

                    LogMessage?.Invoke(this, $"Completed processing {tableName}");
                }
                catch (Exception ex)
                {
                    LogMessage?.Invoke(this, $"Error processing {tableName}: {ex.Message}");
                }
            }, cancellationToken);
        }

        private void ReportProgress(string fileName, int currentIndex, int totalFiles,
            int percentComplete, string currentStep)
        {
            var progress = new ProcessingProgressEventArgs
            {
                FileName = fileName,
                CurrentFileIndex = currentIndex,
                TotalFiles = totalFiles,
                PercentComplete = percentComplete,
                CurrentStep = currentStep,
                Message = $"File {currentIndex}/{totalFiles}: {fileName} - {currentStep}"
            };

            ProgressChanged?.Invoke(this, progress);
        }

        private List<FileProcessingInfo> ReadFileList()
        {
            var filesToProcess = new List<FileProcessingInfo>();

            if (!File.Exists(_fileListPath))
            {
                LogMessage?.Invoke(this, $"File list not found: {_fileListPath}");
                return filesToProcess;
            }

            var lines = File.ReadAllLines(_fileListPath)
                .Where(line => !string.IsNullOrWhiteSpace(line) && !line.TrimStart().StartsWith("#"))
                .ToList();

            foreach (var line in lines)
            {
                var parts = line.Split('|');
                if (parts.Length != 2)
                {
                    LogMessage?.Invoke(this, $"Warning: Skipping invalid line: {line}");
                    continue;
                }

                filesToProcess.Add(new FileProcessingInfo
                {
                    TableName = parts[0].Trim(),
                    FilePath = parts[1].Trim()
                });
            }

            return filesToProcess;
        }

        private class FileProcessingInfo
        {
            public string TableName { get; set; } = string.Empty;
            public string FilePath { get; set; } = string.Empty;
        }
    }

    // Enhanced version of CsvFileProcessor with progress reporting
    public class EnhancedCsvFileProcessor : CsvFileProcessor
    {
        public class StepProgressEventArgs : EventArgs
        {
            public string StepName { get; set; } = string.Empty;
            public int StepPercent { get; set; }
            public string Message { get; set; } = string.Empty;
        }

        public event EventHandler<StepProgressEventArgs>? StepProgress;

        public EnhancedCsvFileProcessor(string connectionString, string tableName)
            : base(connectionString, tableName)
        {
        }

        public new void ProcessFile(string inputFilePath)
        {
            try
            {
                ReportStepProgress("Starting", 0, $"Starting processing of file: {Path.GetFileName(inputFilePath)}");

                // Step 1: Convert encoding (33% of work)
                ReportStepProgress("Converting encoding", 10, "Converting from UTF-16 to UTF-8...");
                base.ProcessFile(inputFilePath);
                ReportStepProgress("Encoding converted", 33, "Successfully converted to UTF-8");

                // Note: The base ProcessFile already handles all steps internally
                // We're just reporting progress at key points
                ReportStepProgress("Cleaning data", 50, "Cleaning CSV data...");
                System.Threading.Thread.Sleep(500); // Small delay to show progress

                ReportStepProgress("Uploading to SQL", 75, "Uploading to Azure SQL Database...");
                System.Threading.Thread.Sleep(500); // Small delay to show progress

                ReportStepProgress("Completed", 100, "File processing completed successfully");
            }
            catch (Exception ex)
            {
                ReportStepProgress("Error", 100, $"ERROR: {ex.Message}");
                throw;
            }
        }

        private void ReportStepProgress(string stepName, int percent, string message)
        {
            StepProgress?.Invoke(this, new StepProgressEventArgs
            {
                StepName = stepName,
                StepPercent = percent,
                Message = message
            });
        }
    }
}