using Parquet;
using ParquetSchema = global::Parquet.Schema.ParquetSchema;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetConfigurationTests
{
    #region Default Values

    [Fact]
    public void ParquetConfiguration_DefaultValues_AreCorrect()
    {
        // Arrange & Act
        var config = new ParquetConfiguration();

        // Assert
        config.RowGroupSize.Should().Be(50_000);
        config.Compression.Should().Be(CompressionMethod.Snappy);
        config.TargetFileSizeBytes.Should().Be(256L * 1024 * 1024);
        config.UseAtomicWrite.Should().BeTrue();
        config.MaxBufferedRows.Should().Be(250_000);
        config.ProjectedColumns.Should().BeNull();
        config.SchemaValidator.Should().BeNull();
        config.SchemaCompatibility.Should().Be(SchemaCompatibilityMode.Strict);
        config.RecursiveDiscovery.Should().BeFalse();
        config.FileReadParallelism.Should().Be(1);
        config.RowFilter.Should().BeNull();
        config.RowErrorHandler.Should().BeNull();
        config.Observer.Should().BeNull();
    }

    #endregion

    #region RowGroupSize Validation

    [Fact]
    public void Validate_RowGroupSizeZero_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = new ParquetConfiguration { RowGroupSize = 0 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*RowGroupSize*greater than 0*");
    }

    [Fact]
    public void Validate_RowGroupSizeNegative_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = new ParquetConfiguration { RowGroupSize = -1 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*RowGroupSize*greater than 0*");
    }

    [Fact]
    public void Validate_RowGroupSizeOne_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration { RowGroupSize = 1 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_RowGroupSizeLarge_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration { RowGroupSize = 1_000_000, MaxBufferedRows = 1_000_000 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    #endregion

    #region MaxBufferedRows Validation

    [Fact]
    public void Validate_MaxBufferedRowsLessThanRowGroupSize_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = new ParquetConfiguration
        {
            RowGroupSize = 100_000,
            MaxBufferedRows = 50_000
        };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*MaxBufferedRows*greater than or equal to*RowGroupSize*");
    }

    [Fact]
    public void Validate_MaxBufferedRowsEqualToRowGroupSize_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration
        {
            RowGroupSize = 100_000,
            MaxBufferedRows = 100_000
        };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_MaxBufferedRowsGreaterThanRowGroupSize_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration
        {
            RowGroupSize = 100_000,
            MaxBufferedRows = 200_000
        };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    #endregion

    #region ProjectedColumns

    [Fact]
    public void ProjectedColumns_CanBeSetToNull()
    {
        // Arrange & Act
        var config = new ParquetConfiguration { ProjectedColumns = null };

        // Assert
        config.ProjectedColumns.Should().BeNull();
    }

    [Fact]
    public void ProjectedColumns_CanBeSetToEmptyList()
    {
        // Arrange & Act
        var config = new ParquetConfiguration { ProjectedColumns = [] };

        // Assert
        config.ProjectedColumns.Should().BeEmpty();
    }

    [Fact]
    public void ProjectedColumns_CanBeSetToListOfColumns()
    {
        // Arrange & Act
        var config = new ParquetConfiguration { ProjectedColumns = ["Col1", "Col2"] };

        // Assert
        config.ProjectedColumns.Should().BeEquivalentTo("Col1", "Col2");
    }

    #endregion

    #region Compression Enum Values

    [Theory]
    [InlineData(CompressionMethod.None)]
    [InlineData(CompressionMethod.Snappy)]
    [InlineData(CompressionMethod.Gzip)]
    public void Compression_AllValidValuesCanBeSet(CompressionMethod compression)
    {
        // Arrange & Act
        var config = new ParquetConfiguration { Compression = compression };

        // Assert
        config.Compression.Should().Be(compression);
    }

    #endregion

    #region TargetFileSizeBytes Validation

    [Fact]
    public void Validate_TargetFileSizeBytesNull_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration { TargetFileSizeBytes = null };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_TargetFileSizeBytesBelow32MiB_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = new ParquetConfiguration { TargetFileSizeBytes = 16L * 1024 * 1024 }; // 16 MiB

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*TargetFileSizeBytes*at least 32 MiB*");
    }

    [Fact]
    public void Validate_TargetFileSizeBytesExactly32MiB_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration { TargetFileSizeBytes = 32L * 1024 * 1024 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_TargetFileSizeBytesAbove32MiB_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration { TargetFileSizeBytes = 128L * 1024 * 1024 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    #endregion

    #region FileReadParallelism Validation

    [Fact]
    public void Validate_FileReadParallelismZero_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = new ParquetConfiguration { FileReadParallelism = 0 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*FileReadParallelism*greater than or equal to 1*");
    }

    [Fact]
    public void Validate_FileReadParallelismOne_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration { FileReadParallelism = 1 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_FileReadParallelismGreaterThanOne_DoesNotThrow()
    {
        // Arrange
        var config = new ParquetConfiguration { FileReadParallelism = 4 };

        // Act
        Action act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    #endregion

    #region SchemaCompatibilityMode

    [Theory]
    [InlineData(SchemaCompatibilityMode.Strict)]
    [InlineData(SchemaCompatibilityMode.Additive)]
    [InlineData(SchemaCompatibilityMode.NameOnly)]
    public void SchemaCompatibility_AllValidValuesCanBeSet(SchemaCompatibilityMode mode)
    {
        // Arrange & Act
        var config = new ParquetConfiguration { SchemaCompatibility = mode };

        // Assert
        config.SchemaCompatibility.Should().Be(mode);
    }

    #endregion

    #region Custom Configuration

    [Fact]
    public void ParquetConfiguration_CanSetAllProperties()
    {
        // Arrange
        Func<ParquetSchema, bool> schemaValidator = _ => true;
        Func<ParquetRow, bool> rowFilter = _ => true;
        Func<Exception, ParquetRow, bool> errorHandler = (_, _) => true;
        var observer = new TestParquetConnectorObserver();

        // Act
        var config = new ParquetConfiguration
        {
            RowGroupSize = 25_000,
            Compression = CompressionMethod.Gzip,
            TargetFileSizeBytes = 512L * 1024 * 1024,
            UseAtomicWrite = false,
            MaxBufferedRows = 100_000,
            ProjectedColumns = ["Col1"],
            SchemaValidator = schemaValidator,
            SchemaCompatibility = SchemaCompatibilityMode.Additive,
            RecursiveDiscovery = true,
            RowFilter = rowFilter,
            RowErrorHandler = errorHandler,
            Observer = observer
        };
#pragma warning disable CS0618 // Type or member is obsolete
        config.FileReadParallelism = 2; // Reserved for future use
#pragma warning restore CS0618

        // Assert
        config.RowGroupSize.Should().Be(25_000);
        config.Compression.Should().Be(CompressionMethod.Gzip);
        config.TargetFileSizeBytes.Should().Be(512L * 1024 * 1024);
        config.UseAtomicWrite.Should().BeFalse();
        config.MaxBufferedRows.Should().Be(100_000);
        config.ProjectedColumns.Should().BeEquivalentTo("Col1");
        config.SchemaValidator.Should().Be(schemaValidator);
        config.SchemaCompatibility.Should().Be(SchemaCompatibilityMode.Additive);
        config.RecursiveDiscovery.Should().BeTrue();
#pragma warning disable CS0618 // Type or member is obsolete
        config.FileReadParallelism.Should().Be(2); // Reserved for future use
#pragma warning restore CS0618
        config.RowFilter.Should().Be(rowFilter);
        config.RowErrorHandler.Should().Be(errorHandler);
        config.Observer.Should().Be(observer);
    }

    #endregion

    #region Test Helpers

    private sealed class TestParquetConnectorObserver : IParquetConnectorObserver
    {
        public void OnFileReadStarted(StorageProviders.Models.StorageUri fileUri) { }
        public void OnRowGroupRead(StorageProviders.Models.StorageUri fileUri, int rowGroupIndex, long rowCount) { }
        public void OnFileReadCompleted(StorageProviders.Models.StorageUri fileUri, long totalRows, long totalBytes, TimeSpan duration) { }
        public void OnRowMappingError(StorageProviders.Models.StorageUri fileUri, Exception exception) { }
        public void OnRowGroupWritten(StorageProviders.Models.StorageUri fileUri, int rowGroupIndex, long rowCount) { }
        public void OnFileWriteCompleted(StorageProviders.Models.StorageUri fileUri, long totalRows, long totalBytes, TimeSpan duration) { }
    }

    #endregion
}
