namespace NPipeline.Connectors.DuckDB.Configuration;

/// <summary>
///     Options for file export via DuckDB's COPY TO statement.
/// </summary>
public sealed class DuckDBFileExportOptions
{
    /// <summary>
    ///     Output format: parquet, csv, json. Auto-detected from file extension if null.
    /// </summary>
    public string? Format { get; set; }

    /// <summary>
    ///     Compression for Parquet/CSV output: zstd, snappy, gzip, none.
    ///     Default: zstd for Parquet, none for CSV.
    /// </summary>
    public string? Compression { get; set; }

    /// <summary>
    ///     CSV delimiter character. Default: ','.
    /// </summary>
    public char CsvDelimiter { get; set; } = ',';

    /// <summary>
    ///     Include header row in CSV output. Default: true.
    /// </summary>
    public bool CsvHeader { get; set; } = true;

    /// <summary>
    ///     Parquet row group size. Default: 122880 (DuckDB default).
    /// </summary>
    public int ParquetRowGroupSize { get; set; } = 122880;

    /// <summary>
    ///     Builds the COPY TO options clause for the given file path.
    /// </summary>
    internal string BuildCopyOptions(string filePath)
    {
        var format = Format ?? InferFormat(filePath);
        var parts = new List<string> { $"FORMAT {format.ToUpperInvariant()}" };

        if (!string.IsNullOrEmpty(Compression))
            parts.Add($"COMPRESSION '{Compression}'");

        if (format.Equals("csv", StringComparison.OrdinalIgnoreCase))
        {
            if (CsvDelimiter != ',')
                parts.Add($"DELIMITER '{CsvDelimiter}'");

            parts.Add($"HEADER {(CsvHeader ? "true" : "false")}");
        }

        if (format.Equals("parquet", StringComparison.OrdinalIgnoreCase) && ParquetRowGroupSize != 122880)
            parts.Add($"ROW_GROUP_SIZE {ParquetRowGroupSize}");

        return string.Join(", ", parts);
    }

    private static string InferFormat(string filePath)
    {
        var ext = Path.GetExtension(filePath).ToLowerInvariant();

        return ext switch
        {
            ".parquet" => "parquet",
            ".csv" or ".tsv" => "csv",
            ".json" or ".ndjson" or ".jsonl" => "json",
            _ => throw new InvalidOperationException(
                $"Cannot infer file format from extension '{ext}'. Set Format explicitly."),
        };
    }
}
