using System.Globalization;
using CsvHelper;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Configuration for CSV operations that wraps CsvHelper's CsvConfiguration
///     with additional NPipeline-specific settings.
/// </summary>
public class CsvConfiguration
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="CsvConfiguration" /> class.
    /// </summary>
    public CsvConfiguration()
        : this(CultureInfo.InvariantCulture)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CsvConfiguration" /> class.
    /// </summary>
    /// <param name="cultureInfo">The culture information to use.</param>
    public CsvConfiguration(CultureInfo cultureInfo)
    {
        HelperConfiguration = new CsvHelper.Configuration.CsvConfiguration(cultureInfo);
    }

    /// <summary>
    ///     Gets or sets the buffer size for the StreamWriter used in CSV operations.
    ///     Default value is 4096 (4KB), which provides good performance for most scenarios.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The buffer size affects the efficiency of CSV I/O operations. Larger buffers reduce the number
    ///         of system calls and can improve throughput, but consume more memory. The default of 4KB balances
    ///         performance and memory usage for typical CSV processing workloads.
    ///     </para>
    ///     <para>
    ///         For high-throughput scenarios, consider increasing to 8192 (8KB) or higher. For memory-constrained
    ///         environments, smaller values can be used.
    ///     </para>
    /// </remarks>
    public int BufferSize { get; set; } = 4096;

    /// <summary>
    ///     Gets or sets a value indicating whether the CSV file has a header record.
    ///     Default value is <c>true</c>.
    /// </summary>
    /// <remarks>
    ///     This is a convenience property that wraps <see cref="CsvHelper.Configuration.CsvConfiguration.HasHeaderRecord" />.
    /// </remarks>
    public bool HasHeaderRecord
    {
        get => HelperConfiguration.HasHeaderRecord;
        set => HelperConfiguration.HasHeaderRecord = value;
    }

    /// <summary>
    ///     Gets or sets the header validation callback for handling validation errors.
    ///     Default value is <c>null</c> to use CsvHelper's default behavior.
    /// </summary>
    /// <remarks>
    ///     This is a convenience property that wraps <see cref="CsvHelper.Configuration.CsvConfiguration.HeaderValidated" />.
    ///     Set to <c>null</c> to ignore header validation errors, or provide a custom validation function.
    /// </remarks>
    public HeaderValidated? HeaderValidated
    {
        get => HelperConfiguration.HeaderValidated;
        set => HelperConfiguration.HeaderValidated = value;
    }

    /// <summary>
    ///     Gets the underlying CsvHelper configuration.
    /// </summary>
    public CsvHelper.Configuration.CsvConfiguration HelperConfiguration { get; }

    /// <summary>
    ///     Optional handler invoked when a row mapping throws in <see cref="CsvSourceNode{T}" />.
    ///     Return true to skip the row and continue; return false or rethrow to fail the pipeline.
    /// </summary>
    public Func<Exception, CsvRow, bool>? RowErrorHandler { get; set; }

    /// <summary>
    ///     Implicit conversion to CsvHelper's CsvConfiguration for compatibility.
    /// </summary>
    /// <param name="config">The NPipeline CSV configuration.</param>
    /// <returns>The CsvHelper configuration.</returns>
    public static implicit operator CsvHelper.Configuration.CsvConfiguration(CsvConfiguration config)
    {
        return config.HelperConfiguration;
    }
}
