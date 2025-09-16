using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace CsvProcessor
{
    public class CsvFileProcessor
    {
        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly List<string> _fixedAutomationNames;
        private readonly string _logFilePath;
        private readonly object _logLock = new object();

        public CsvFileProcessor(string connectionString, string tableName)
        {
            _connectionString = connectionString;
            _tableName = tableName;
            _fixedAutomationNames = new List<string>();

            // Create log file with timestamp in filename
            string logDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs");
            Directory.CreateDirectory(logDirectory);
            _logFilePath = Path.Combine(logDirectory, $"CsvProcessor_{tableName}_{DateTime.Now:yyyyMMdd_HHmmss}.log");
        }

        private void Log(string message)
        {
            lock (_logLock)
            {
                string logEntry = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";

                // Write to console
                Console.WriteLine(logEntry);

                // Write to file
                try
                {
                    File.AppendAllText(_logFilePath, logEntry + Environment.NewLine);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to write to log file: {ex.Message}");
                }
            }
        }

        public void ProcessFile(string inputFilePath)
        {
            try
            {
                Log($"Starting processing of file: {inputFilePath}");
                Log($"Log file location: {_logFilePath}");

                // Step 1: Convert encoding
                string utf8FilePath = ConvertToUtf8(inputFilePath);

                // Step 2: Clean data
                string cleanedFilePath = CleanCsvData(utf8FilePath);

                // Step 3: Upload to Azure SQL
                UploadToAzureSql(cleanedFilePath);

                // Display fixed automation names
                // DisplayFixedAutomationNames();
            }
            catch (Exception ex)
            {
                Log($"ERROR: {ex.Message}");
                Log($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private string ConvertToUtf8(string inputFilePath)
        {
            Log("\n=== STEP 1: ENCODING CONVERSION ===");

            string outputFilePath = Path.Combine(
                Path.GetDirectoryName(inputFilePath) ?? "",
                Path.GetFileNameWithoutExtension(inputFilePath) + "_utf8" + Path.GetExtension(inputFilePath)
            );

            Log($"Input file: {inputFilePath}");
            Log($"Output file: {outputFilePath}");

            // Read with UTF-16 LE BOM encoding
            Encoding sourceEncoding = Encoding.Unicode; // UTF-16 LE
            string fileContent = File.ReadAllText(inputFilePath, sourceEncoding);

            // Write with UTF-8 encoding
            File.WriteAllText(outputFilePath, fileContent, new UTF8Encoding(true));

            Log($"Successfully converted from UTF-16 LE BOM to UTF-8");
            Log($"File size: {new FileInfo(outputFilePath).Length:N0} bytes");

            return outputFilePath;
        }

        private string CleanCsvData(string inputFilePath)
        {
            Log("\n=== STEP 2: DATA CLEANING ===");

            string outputFilePath = Path.Combine(
                Path.GetDirectoryName(inputFilePath) ?? "",
                Path.GetFileNameWithoutExtension(inputFilePath) + "_cleaned" + Path.GetExtension(inputFilePath)
            );

            Log($"Input file: {inputFilePath}");
            Log($"Output file: {outputFilePath}");

            List<string[]> cleanedRows = new List<string[]>();
            string[]? headers = null;
            int totalRowsProcessed = 0;
            int rowsCleaned = 0;
            int automationNameColumnIndex = -1;

            using (StreamReader reader = new StreamReader(inputFilePath, Encoding.UTF8))
            {
                string? line;
                string[]? currentRow = null;

                while ((line = reader.ReadLine()) != null)
                {
                    bool fixedRow = false;
                    string originalColValue = "";
                    string fixedColValue = "";
                    int brokenIndex = -1;

                    totalRowsProcessed++;

                    if (headers == null)
                    {
                        // First line contains headers
                        headers = ParseCsvLine(line, true, Path.GetFileName(inputFilePath), true);
                        cleanedRows.Add(headers);

                        // Find AutomationName column index
                        for (int i = 0; i < headers.Length; i++)
                        {
                            if (headers[i].Trim().Equals("AutomationName", StringComparison.OrdinalIgnoreCase))
                            {
                                automationNameColumnIndex = i;
                                break;
                            }
                        }

                        Log($"Number of columns identified: {headers.Length}");
                        Log($"Column names: {string.Join(", ", headers)}");
                        Log($"AutomationName column index: {automationNameColumnIndex}");
                        continue;
                    }

                    string[] parsedLine = ParseCsvLine(line, false, Path.GetFileName(inputFilePath), true);
                    originalColValue = parsedLine[parsedLine.Length - 1].Trim();

                    //Kal - code to fix the line, so the rest of the code might not need to handle broken lines...
                    while (parsedLine.Length < headers.Length)
                    {
                        //Houston we have a problem, not enough columns
                        fixedRow = true;
                        if (brokenIndex < 0)
                        {
                            originalColValue = parsedLine[parsedLine.Length - 3].Trim(); // this is to account for the 2 extra cols being added
                            brokenIndex = parsedLine.Length - 3;
                        }

                        //read the next line
                        var line2 = reader.ReadLine();
                        totalRowsProcessed++;
                        rowsCleaned++;

                        //concatenate the lines
                        line = line + line2;

                        parsedLine = ParseCsvLine(line, false, Path.GetFileName(inputFilePath), true);
                        fixedColValue = parsedLine[brokenIndex];
                    }

                    if (fixedRow == true)
                    {
                        _fixedAutomationNames.Add($"Row {totalRowsProcessed}: '{originalColValue}' -> '{fixedColValue}'");
                        brokenIndex = -1;
                        originalColValue = "";
                        fixedColValue = "";
                        fixedRow = false;
                    }

                    currentRow = parsedLine;

                    // Add the cleaned row
                    cleanedRows.Add(currentRow);
                }
            }

            // Write cleaned data
            using (StreamWriter writer = new StreamWriter(outputFilePath, false, Encoding.UTF8))
            {
                foreach (var row in cleanedRows)
                {
                    writer.WriteLine(BuildCsvLine(row));
                }
            }

            Log($"Total rows processed: {totalRowsProcessed}");
            Log($"Number of rows cleaned: {rowsCleaned}");
            Log($"Final row count: {cleanedRows.Count}");

            return outputFilePath;
        }

        private void UploadToAzureSql(string csvFilePath)
        {
            Log("\n=== STEP 3: AZURE SQL UPLOAD ===");

            DataTable dataTable = LoadCsvToDataTable(csvFilePath);
            int recordsUploaded = 0;

            Log($"Loaded {dataTable.Rows.Count} records into DataTable");
            Log($"Connecting to Azure SQL Database...");

            using (Microsoft.Data.SqlClient.SqlConnection connection = new Microsoft.Data.SqlClient.SqlConnection(_connectionString))
            {
                connection.Open();
                Log("Connected successfully");

                // Create temporary table for bulk insert
                string tempTableName = $"#{_tableName}_temp_{Guid.NewGuid():N}";
                CreateTempTable(connection, tempTableName, dataTable);

                // Bulk insert into temp table
                using (Microsoft.Data.SqlClient.SqlBulkCopy bulkCopy = new Microsoft.Data.SqlClient.SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.BulkCopyTimeout = 300; // 5 minutes timeout

                    // Map columns
                    foreach (DataColumn column in dataTable.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }

                    bulkCopy.WriteToServer(dataTable);
                    Log($"Bulk insert completed: {dataTable.Rows.Count} records inserted into temp table");
                }

                // Perform UPSERT operation
                recordsUploaded = PerformUpsert(connection, tempTableName, dataTable);
                Log($"UPSERT operation completed: {recordsUploaded} records affected");
            }

            Log($"Total records uploaded: {recordsUploaded}");
        }

        private void CreateTempTable(Microsoft.Data.SqlClient.SqlConnection connection, string tempTableName, DataTable dataTable)
        {
            StringBuilder createTableSql = new StringBuilder($"CREATE TABLE {tempTableName} (");

            foreach (DataColumn column in dataTable.Columns)
            {
                string sqlType = GetSqlType(column);
                createTableSql.Append($"[{column.ColumnName}] {sqlType}, ");
            }

            createTableSql.Length -= 2; // Remove last comma and space
            createTableSql.Append(")");

            using (Microsoft.Data.SqlClient.SqlCommand command = new Microsoft.Data.SqlClient.SqlCommand(createTableSql.ToString(), connection))
            {
                command.ExecuteNonQuery();
            }
        }

        private int PerformUpsert(Microsoft.Data.SqlClient.SqlConnection connection, string tempTableName, DataTable dataTable)
        {
            // Assuming the first column is the primary key for UPSERT
            string primaryKeyColumn = dataTable.Columns[0].ColumnName;

            StringBuilder upsertSql = new StringBuilder();
            upsertSql.AppendLine($"MERGE {_tableName} AS target");
            upsertSql.AppendLine($"USING {tempTableName} AS source");
            upsertSql.AppendLine($"ON target.[{primaryKeyColumn}] = source.[{primaryKeyColumn}]");

            // Update clause
            upsertSql.AppendLine("WHEN MATCHED THEN UPDATE SET");
            List<string> updateColumns = new List<string>();
            foreach (DataColumn column in dataTable.Columns)
            {
                if (column.ColumnName != primaryKeyColumn)
                {
                    updateColumns.Add($"target.[{column.ColumnName}] = source.[{column.ColumnName}]");
                }
            }
            upsertSql.AppendLine(string.Join(", ", updateColumns));

            // Insert clause
            upsertSql.AppendLine("WHEN NOT MATCHED BY TARGET THEN");
            upsertSql.Append("INSERT (");
            upsertSql.Append(string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(c => $"[{c.ColumnName}]")));
            upsertSql.AppendLine(")");
            upsertSql.Append("VALUES (");
            upsertSql.Append(string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(c => $"source.[{c.ColumnName}]")));
            upsertSql.AppendLine(");");

            using (Microsoft.Data.SqlClient.SqlCommand command = new Microsoft.Data.SqlClient.SqlCommand(upsertSql.ToString(), connection))
            {
                command.CommandTimeout = 500;
                return command.ExecuteNonQuery();
            }
        }

        private DataTable LoadCsvToDataTable(string csvFilePath)
        {
            DataTable dataTable = new DataTable();

            using (StreamReader reader = new StreamReader(csvFilePath, Encoding.UTF8))
            {
                string? headerLine = reader.ReadLine();
                if (headerLine != null)
                {
                    string[] headers = ParseCsvLine(headerLine, true, "", false);

                    // Create columns
                    foreach (string header in headers)
                    {
                        dataTable.Columns.Add(header.Trim());
                    }

                    // Read data
                    string? line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] values = ParseCsvLine(line, false, "", false);

                        // Ensure we have the right number of values
                        if (values.Length == headers.Length)
                        {
                            dataTable.Rows.Add(values);
                        }
                    }
                }
            }

            return dataTable;
        }

        private string[] ParseCsvLine(string line, bool isHeader, string csvfilename, bool isOriginalFile)
        {
            List<string> result = new List<string>();
            StringBuilder currentField = new StringBuilder();

            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];

                if (c == '|')
                {
                    result.Add(currentField.ToString());
                    currentField.Clear();
                }
                else
                {
                    currentField.Append(c);
                }
            }

            result.Add(currentField.ToString());

            if (isOriginalFile)
            {
                if (isHeader)
                {
                    //add headers
                    result.Add("FileName");
                    result.Add("FileLoadDate");
                }
                else
                {
                    //add values
                    result.Add(csvfilename);
                    result.Add($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                }
            }

            return result.ToArray();
        }

        private string BuildCsvLine(string[] values)
        {
            return string.Join("|", values.Select(v =>
            {
                if (v.Contains("\"") || v.Contains("\n"))
                {
                    return $"\"{v.Replace("\"", "\"\"")}\"";
                }
                return v;
            }));
        }

        private string GetSqlType(DataColumn column)
        {
            // Default to NVARCHAR(MAX) for simplicity
            return "NVARCHAR(MAX)";
        }
    }
}