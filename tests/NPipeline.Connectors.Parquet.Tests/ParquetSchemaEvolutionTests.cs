using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.Connectors.Parquet.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetSchemaEvolutionTests
{
    #region Extra Columns in File

    [Fact]
    public async Task Read_WithExtraColumnsInFile_IgnoresExtraColumnsInAdditiveMode()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write with extra columns
            var fullRecords = new[]
            {
                new FullRecord { Id = 1, Name = "Test", ExtraColumn = "Extra" },
            };

            var sink = new ParquetSinkNode<FullRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<FullRecord>(fullRecords.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with schema that doesn't have the extra column
            var config = new ParquetConfiguration { SchemaCompatibility = SchemaCompatibilityMode.Additive };
            var source = new ParquetSourceNode<PartialRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - extra column should be ignored
            result.Should().HaveCount(1);
            result[0].Id.Should().Be(1);
            result[0].Name.Should().Be("Test");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Helper Methods

    private static void CleanupFile(string path)
    {
        if (File.Exists(path))
            File.Delete(path);

        var directory = Path.GetDirectoryName(path);

        if (directory is not null && Directory.Exists(directory))
        {
            var tempFiles = Directory.GetFiles(directory, Path.GetFileName(path) + ".tmp-*");

            foreach (var tempFile in tempFiles)
            {
                try
                {
                    File.Delete(tempFile);
                }
                catch
                {
                    /* ignore */
                }
            }
        }
    }

    #endregion

    #region Strict vs Additive Compatibility Mode

    [Fact]
    public async Task Read_InStrictMode_WithMatchingSchema_Succeeds()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write initial data
            var records = new[] { new SimpleRecord { Id = 1, Name = "Test" } };
            var sink = new ParquetSinkNode<SimpleRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<SimpleRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with strict mode (default)
            var config = new ParquetConfiguration { SchemaCompatibility = SchemaCompatibilityMode.Strict };
            var source = new ParquetSourceNode<SimpleRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(1);
            result[0].Id.Should().Be(1);
            result[0].Name.Should().Be("Test");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Read_InAdditiveMode_WithMissingColumns_UsesDefaults()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write with fewer columns
            var minimalRecords = new[] { new MinimalRecord { Id = 1 } };
            var sink = new ParquetSinkNode<MinimalRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<MinimalRecord>(minimalRecords.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with extended schema in additive mode
            var config = new ParquetConfiguration { SchemaCompatibility = SchemaCompatibilityMode.Additive };
            var source = new ParquetSourceNode<ExtendedRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - missing column should have default value
            result.Should().HaveCount(1);
            result[0].Id.Should().Be(1);
            result[0].Name.Should().BeNull(); // Default for missing string column
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Read_InAdditiveMode_WithExtraColumnsInFile_IgnoresExtraColumns()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write with more columns
            var extendedRecords = new[] { new ExtendedRecord { Id = 1, Name = "Test" } };
            var sink = new ParquetSinkNode<ExtendedRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<ExtendedRecord>(extendedRecords.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with minimal schema in additive mode
            var config = new ParquetConfiguration { SchemaCompatibility = SchemaCompatibilityMode.Additive };
            var source = new ParquetSourceNode<MinimalRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - extra column should be ignored
            result.Should().HaveCount(1);
            result[0].Id.Should().Be(1);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Renamed Columns

    [Fact]
    public async Task Read_WithRenamedColumn_MapsCorrectly()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write with original column name
            var originalRecords = new[] { new OriginalNameRecord { OriginalId = 42 } };
            var sink = new ParquetSinkNode<OriginalNameRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<OriginalNameRecord>(originalRecords.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with renamed column
            var source = new ParquetSourceNode<RenamedColumnRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - should map "original_id" to "NewId"
            result.Should().HaveCount(1);
            result[0].NewId.Should().Be(42);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithRenamedColumn_UsesNewNameInFile()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write with renamed column
            var renamedRecords = new[] { new RenamedColumnRecord { NewId = 100 } };
            var sink = new ParquetSinkNode<RenamedColumnRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<RenamedColumnRecord>(renamedRecords.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read back with original name mapping
            var source = new ParquetSourceNode<OriginalNameRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - should read "original_id" from file
            result.Should().HaveCount(1);
            result[0].OriginalId.Should().Be(100);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Missing Columns Handling

    [Fact]
    public async Task Read_WithMissingNullableColumn_SetsToNull()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write without the optional column
            var requiredOnly = new[] { new RequiredOnlyRecord { RequiredId = 1 } };
            var sink = new ParquetSinkNode<RequiredOnlyRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<RequiredOnlyRecord>(requiredOnly.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with schema that has optional column
            var config = new ParquetConfiguration { SchemaCompatibility = SchemaCompatibilityMode.Additive };
            var source = new ParquetSourceNode<WithOptionalRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(1);
            result[0].RequiredId.Should().Be(1);
            result[0].OptionalName.Should().BeNull();
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Read_WithMissingValueTypeColumn_SetsToDefault()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write without the count column
            var idOnly = new[] { new IdOnlyRecord { Id = 5 } };
            var sink = new ParquetSinkNode<IdOnlyRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<IdOnlyRecord>(idOnly.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with schema that has value type column
            var config = new ParquetConfiguration { SchemaCompatibility = SchemaCompatibilityMode.Additive };
            var source = new ParquetSourceNode<WithCountRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - missing int column should default to 0
            result.Should().HaveCount(1);
            result[0].Id.Should().Be(5);
            result[0].Count.Should().Be(0);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Type Coercion Guardrails

    [Fact]
    public async Task Read_WithCompatibleNumericTypes_CoercesInNameOnlyMode()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write with int
            var intRecords = new[] { new IntValueRecord { Value = 42 } };
            var sink = new ParquetSinkNode<IntValueRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<IntValueRecord>(intRecords.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with long (compatible type)
            var config = new ParquetConfiguration { SchemaCompatibility = SchemaCompatibilityMode.NameOnly };
            var source = new ParquetSourceNode<LongValueRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - int should be readable as long
            result.Should().HaveCount(1);
            result[0].Value.Should().Be(42L);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Read_WithFloatToDouble_CoercesInNameOnlyMode()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write with float
            var floatRecords = new[] { new FloatValueRecord { Value = 3.14f } };
            var sink = new ParquetSinkNode<FloatValueRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<FloatValueRecord>(floatRecords.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with double (compatible type)
            var config = new ParquetConfiguration { SchemaCompatibility = SchemaCompatibilityMode.NameOnly };
            var source = new ParquetSourceNode<DoubleValueRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - float should be readable as double
            result.Should().HaveCount(1);
            result[0].Value.Should().BeApproximately(3.14, 0.001);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Schema Validator

    [Fact]
    public async Task Read_WithSchemaValidator_CallsValidator()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write test data
            var records = new[] { new SimpleRecord { Id = 1, Name = "Test" } };
            var sink = new ParquetSinkNode<SimpleRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<SimpleRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            var validatorCalled = false;

            var config = new ParquetConfiguration
            {
                SchemaValidator = schema =>
                {
                    validatorCalled = true;
                    schema.Fields.Should().HaveCount(2);
                    return true;
                },
            };

            var source = new ParquetSourceNode<SimpleRecord>(uri, resolver, config);
            _ = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            validatorCalled.Should().BeTrue();
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Read_WithSchemaValidatorReturningFalse_ThrowsParquetSchemaException()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write test data
            var records = new[] { new SimpleRecord { Id = 1, Name = "Test" } };
            var sink = new ParquetSinkNode<SimpleRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<SimpleRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            var config = new ParquetConfiguration
            {
                SchemaValidator = _ => false, // Reject schema
            };

            var source = new ParquetSourceNode<SimpleRecord>(uri, resolver, config);
            var act = async () => await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            await act.Should().ThrowAsync<ParquetSchemaException>()
                .WithMessage("*Schema validation failed*");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Test Record Types

    private sealed class SimpleRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private sealed class MinimalRecord
    {
        public int Id { get; set; }
    }

    private sealed class ExtendedRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private sealed class OriginalNameRecord
    {
        [ParquetColumn("original_id")]
        public int OriginalId { get; set; }
    }

    private sealed class RenamedColumnRecord
    {
        [ParquetColumn("original_id")] // Maps to same column name
        public int NewId { get; set; }
    }

    private sealed class RequiredOnlyRecord
    {
        public int RequiredId { get; set; }
    }

    private sealed class WithOptionalRecord
    {
        public int RequiredId { get; set; }
        public string? OptionalName { get; set; }
    }

    private sealed class IdOnlyRecord
    {
        public int Id { get; set; }
    }

    private sealed class WithCountRecord
    {
        public int Id { get; set; }
        public int Count { get; set; }
    }

    private sealed class FullRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public string? ExtraColumn { get; set; }
    }

    private sealed class PartialRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private sealed class IntValueRecord
    {
        public int Value { get; set; }
    }

    private sealed class LongValueRecord
    {
        public long Value { get; set; }
    }

    private sealed class FloatValueRecord
    {
        public float Value { get; set; }
    }

    private sealed class DoubleValueRecord
    {
        public double Value { get; set; }
    }

    #endregion
}
