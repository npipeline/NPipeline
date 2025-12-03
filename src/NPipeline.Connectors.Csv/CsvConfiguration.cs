using System.Globalization;
using CsvHelper.Configuration;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Configuration for CSV operations that wraps CsvHelper's CsvConfiguration
///     with additional NPipeline-specific settings.
/// </summary>
public class CsvConfiguration
{
    /// <summary>
    ///     Gets or sets the buffer size for the StreamWriter used in CSV operations.
    ///     Default value is 1024.
    /// </summary>
    public int BufferSize { get; set; } = 1024;

    /// <summary>
    ///     Gets the underlying CsvHelper configuration.
    /// </summary>
    public CsvHelper.Configuration.CsvConfiguration HelperConfiguration { get; }

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
    ///     Implicit conversion to CsvHelper's CsvConfiguration for compatibility.
    /// </summary>
    /// <param name="config">The NPipeline CSV configuration.</param>
    /// <returns>The CsvHelper configuration.</returns>
    public static implicit operator CsvHelper.Configuration.CsvConfiguration(CsvConfiguration config)
    {
        return config.HelperConfiguration;
    }
}