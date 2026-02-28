using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Exceptions;

namespace NPipeline.Connectors.DuckDB.Tests;

public sealed class DuckDBConfigurationTests
{
    #region Default Values

    [Fact]
    public void DefaultValues_AreCorrect()
    {
        // Arrange & Act
        var config = new DuckDBConfiguration();

        // Assert
        config.DatabasePath.Should().BeNull();
        config.AccessMode.Should().Be(DuckDBAccessMode.Automatic);
        config.MemoryLimit.Should().BeNull();
        config.Threads.Should().Be(0);
        config.TempDirectory.Should().BeNull();
        config.Extensions.Should().BeNull();
        config.Settings.Should().BeNull();
        config.StreamResults.Should().BeTrue();
        config.FetchSize.Should().Be(2048);
        config.ProjectedColumns.Should().BeNull();
        config.CommandTimeout.Should().Be(30);
        config.WriteStrategy.Should().Be(DuckDBWriteStrategy.Appender);
        config.BatchSize.Should().Be(1000);
        config.AutoCreateTable.Should().BeTrue();
        config.TruncateBeforeWrite.Should().BeFalse();
        config.UseTransaction.Should().BeTrue();
        config.FileExportOptions.Should().BeNull();
        config.CaseInsensitiveMapping.Should().BeTrue();
        config.CacheMappingMetadata.Should().BeTrue();
        config.RowErrorHandler.Should().BeNull();
        config.ContinueOnError.Should().BeFalse();
        config.Observer.Should().BeNull();
    }

    #endregion

    #region AccessMode

    [Theory]
    [InlineData(DuckDBAccessMode.Automatic)]
    [InlineData(DuckDBAccessMode.ReadOnly)]
    [InlineData(DuckDBAccessMode.ReadWrite)]
    public void AccessMode_AllValuesCanBeSet(DuckDBAccessMode mode)
    {
        var config = new DuckDBConfiguration { AccessMode = mode };
        config.AccessMode.Should().Be(mode);
    }

    #endregion

    #region WriteStrategy

    [Theory]
    [InlineData(DuckDBWriteStrategy.Appender)]
    [InlineData(DuckDBWriteStrategy.Sql)]
    public void WriteStrategy_AllValuesCanBeSet(DuckDBWriteStrategy strategy)
    {
        var config = new DuckDBConfiguration { WriteStrategy = strategy };
        config.WriteStrategy.Should().Be(strategy);
    }

    #endregion

    #region Custom Configuration

    [Fact]
    public void CanSetAllProperties()
    {
        Func<Exception, long, bool> errorHandler = (_, _) => true;
        var observer = new TestDuckDBObserver();

        var config = new DuckDBConfiguration
        {
            DatabasePath = "/tmp/test.duckdb",
            AccessMode = DuckDBAccessMode.ReadWrite,
            MemoryLimit = "2GB",
            Threads = 4,
            TempDirectory = "/tmp/duckdb",
            Extensions = ["httpfs", "spatial"],
            Settings = new Dictionary<string, string>
            {
                ["s3_region"] = "us-east-1",
            },
            StreamResults = false,
            FetchSize = 4096,
            ProjectedColumns = ["Id", "Name"],
            CommandTimeout = 60,
            WriteStrategy = DuckDBWriteStrategy.Sql,
            BatchSize = 5000,
            AutoCreateTable = false,
            TruncateBeforeWrite = true,
            UseTransaction = false,
            CaseInsensitiveMapping = false,
            CacheMappingMetadata = false,
            RowErrorHandler = errorHandler,
            ContinueOnError = true,
            Observer = observer,
        };

        config.DatabasePath.Should().Be("/tmp/test.duckdb");
        config.AccessMode.Should().Be(DuckDBAccessMode.ReadWrite);
        config.MemoryLimit.Should().Be("2GB");
        config.Threads.Should().Be(4);
        config.TempDirectory.Should().Be("/tmp/duckdb");
        config.Extensions.Should().BeEquivalentTo("httpfs", "spatial");
        config.Settings.Should().ContainKey("s3_region");
        config.StreamResults.Should().BeFalse();
        config.FetchSize.Should().Be(4096);
        config.ProjectedColumns.Should().BeEquivalentTo("Id", "Name");
        config.CommandTimeout.Should().Be(60);
        config.WriteStrategy.Should().Be(DuckDBWriteStrategy.Sql);
        config.BatchSize.Should().Be(5000);
        config.AutoCreateTable.Should().BeFalse();
        config.TruncateBeforeWrite.Should().BeTrue();
        config.UseTransaction.Should().BeFalse();
        config.CaseInsensitiveMapping.Should().BeFalse();
        config.CacheMappingMetadata.Should().BeFalse();
        config.RowErrorHandler.Should().BeSameAs(errorHandler);
        config.ContinueOnError.Should().BeTrue();
        config.Observer.Should().BeSameAs(observer);
    }

    #endregion

    #region Validate

    [Fact]
    public void Validate_DefaultConfig_DoesNotThrow()
    {
        var config = new DuckDBConfiguration();
        config.Invoking(c => c.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Validate_NegativeBatchSize_Throws()
    {
        var config = new DuckDBConfiguration { BatchSize = -1 };
        config.Invoking(c => c.Validate()).Should().Throw<DuckDBConnectorException>();
    }

    [Fact]
    public void Validate_NegativeFetchSize_Throws()
    {
        var config = new DuckDBConfiguration { FetchSize = -1 };
        config.Invoking(c => c.Validate()).Should().Throw<DuckDBConnectorException>();
    }

    [Fact]
    public void Validate_NegativeCommandTimeout_Throws()
    {
        var config = new DuckDBConfiguration { CommandTimeout = -1 };
        config.Invoking(c => c.Validate()).Should().Throw<DuckDBConnectorException>();
    }

    #endregion

    #region DuckDBOptions

    [Fact]
    public void DuckDBOptions_DefaultConfiguration_IsNotNull()
    {
        var options = new DuckDBOptions();
        options.DefaultConfiguration.Should().NotBeNull();
        options.NamedDatabases.Should().NotBeNull().And.BeEmpty();
    }

    [Fact]
    public void DuckDBOptions_NamedDatabases_CanBeAdded()
    {
        var options = new DuckDBOptions();

        options.NamedDatabases["analytics"] = new DuckDBConfiguration
        {
            DatabasePath = "/data/analytics.duckdb",
            AccessMode = DuckDBAccessMode.ReadOnly,
        };

        options.NamedDatabases.Should().ContainKey("analytics");
        options.NamedDatabases["analytics"].DatabasePath.Should().Be("/data/analytics.duckdb");
    }

    #endregion

    #region FileExportOptions

    [Fact]
    public void FileExportOptions_DefaultValues()
    {
        var opts = new DuckDBFileExportOptions();
        opts.Format.Should().BeNull();
        opts.Compression.Should().BeNull();
        opts.CsvDelimiter.Should().Be(',');
        opts.CsvHeader.Should().BeTrue();
        opts.ParquetRowGroupSize.Should().Be(122880);
    }

    [Fact]
    public void FileExportOptions_BuildCopyOptions_InfersFormatFromExtension()
    {
        var opts = new DuckDBFileExportOptions();
        var result = opts.BuildCopyOptions("output.parquet");
        result.Should().Contain("FORMAT PARQUET");
    }

    [Fact]
    public void FileExportOptions_BuildCopyOptions_CsvFormat()
    {
        var opts = new DuckDBFileExportOptions
        {
            CsvDelimiter = '|',
            CsvHeader = true,
        };

        var result = opts.BuildCopyOptions("output.csv");
        result.Should().Contain("FORMAT CSV");
        result.Should().Contain("DELIMITER '|'");
        result.Should().Contain("HEADER true");
    }

    [Fact]
    public void FileExportOptions_ExplicitFormat_OverridesExtension()
    {
        var opts = new DuckDBFileExportOptions { Format = "JSON" };
        var result = opts.BuildCopyOptions("output.txt");
        result.Should().Contain("FORMAT JSON");
    }

    #endregion
}

/// <summary>
///     Minimal observer implementation for testing.
/// </summary>
internal sealed class TestDuckDBObserver : IDuckDBConnectorObserver
{
    public int RowsRead { get; private set; }
    public int RowsWritten { get; private set; }
    public long? ReadCompletedCount { get; private set; }
    public long? WriteCompletedCount { get; private set; }
    public List<string> ExtensionsLoaded { get; } = [];

    public void OnRowRead(long rowIndex)
    {
        RowsRead++;
    }

    public void OnRowWritten(long rowIndex)
    {
        RowsWritten++;
    }

    public void OnReadCompleted(long totalRows)
    {
        ReadCompletedCount = totalRows;
    }

    public void OnWriteCompleted(long totalRows)
    {
        WriteCompletedCount = totalRows;
    }

    public void OnExtensionLoaded(string extensionName)
    {
        ExtensionsLoaded.Add(extensionName);
    }
}
