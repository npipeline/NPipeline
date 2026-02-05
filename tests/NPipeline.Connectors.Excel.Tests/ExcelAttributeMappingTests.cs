using AwesomeAssertions;
using NPipeline.Connectors.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Excel.Tests;

/// <summary>
///     Integration tests for attribute-based mapping with ExcelSource and ExcelSink nodes.
///     Validates end-to-end functionality of the attribute mapping feature.
/// </summary>
public sealed class ExcelAttributeMappingTests
{
    #region Convention-Based Mapping Tests

    [Fact]
    public async Task ExcelSource_WithConventionMapping_ReadsDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new SimplePoco { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new SimplePoco { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<SimplePoco>(uri, resolver, config);
            IDataPipe<SimplePoco> input = new StreamingDataPipe<SimplePoco>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<SimplePoco>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<SimplePoco>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[0].CreatedAt.Should().Be(new DateTime(2024, 1, 1));
            results[0].IsActive.Should().BeTrue();
            results[0].Amount.Should().Be(100.50m);

            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
            results[1].CreatedAt.Should().Be(new DateTime(2024, 1, 2));
            results[1].IsActive.Should().BeFalse();
            results[1].Amount.Should().Be(200.75m);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Attribute-Based Mapping Tests

    [Fact]
    public async Task ExcelSource_WithAttributeMapping_ReadsDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithAttributes { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new PocoWithAttributes { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithAttributes>(uri, resolver, config);
            IDataPipe<PocoWithAttributes> input = new StreamingDataPipe<PocoWithAttributes>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithAttributes>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithAttributes>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[0].CreatedAt.Should().Be(new DateTime(2024, 1, 1));
            results[0].IsActive.Should().BeTrue();
            results[0].Amount.Should().Be(100.50m);

            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
            results[1].CreatedAt.Should().Be(new DateTime(2024, 1, 2));
            results[1].IsActive.Should().BeFalse();
            results[1].Amount.Should().Be(200.75m);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Ignore Attribute Tests

    [Fact]
    public async Task ExcelSource_WithIgnoreAttribute_ExcludesIgnoredProperties()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithIgnore { Id = 1, IgnoredProperty = "Should not appear", Name = "John Doe", AlsoIgnored = "Also should not appear" },
                new PocoWithIgnore { Id = 2, IgnoredProperty = "Should not appear", Name = "Jane Doe", AlsoIgnored = "Also should not appear" },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithIgnore>(uri, resolver, config);
            IDataPipe<PocoWithIgnore> input = new StreamingDataPipe<PocoWithIgnore>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithIgnore>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithIgnore>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[0].IgnoredProperty.Should().BeEmpty();
            results[0].AlsoIgnored.Should().BeEmpty();

            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
            results[1].IgnoredProperty.Should().BeEmpty();
            results[1].AlsoIgnored.Should().BeEmpty();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Case-Insensitive Column Matching Tests

    [Fact]
    public async Task ExcelSource_WithDifferentColumnCase_MapsSuccessfully()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithMixedCase { UserId = 1, FullName = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true },
                new PocoWithMixedCase { UserId = 2, FullName = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithMixedCase>(uri, resolver, config);
            IDataPipe<PocoWithMixedCase> input = new StreamingDataPipe<PocoWithMixedCase>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithMixedCase>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithMixedCase>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].UserId.Should().Be(1);
            results[0].FullName.Should().Be("John Doe");
            results[0].IsActive.Should().BeTrue();

            results[1].UserId.Should().Be(2);
            results[1].FullName.Should().Be("Jane Doe");
            results[1].IsActive.Should().BeFalse();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Nullable Types Tests

    [Fact]
    public async Task ExcelSource_WithNullableTypes_HandlesNullValues()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithNullableTypes { Id = 1, Name = "John", CreatedAt = null, IsActive = true, Amount = null },
                new PocoWithNullableTypes { Id = 2, Name = "Jane", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithNullableTypes>(uri, resolver, config);
            IDataPipe<PocoWithNullableTypes> input = new StreamingDataPipe<PocoWithNullableTypes>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithNullableTypes>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithNullableTypes>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John");
            results[0].CreatedAt.Should().BeNull();
            results[0].IsActive.Should().BeTrue();
            results[0].Amount.Should().BeNull();

            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane");
            results[1].CreatedAt.Should().Be(new DateTime(2024, 1, 2));
            results[1].IsActive.Should().BeFalse();
            results[1].Amount.Should().Be(200.75m);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Various Types Tests

    [Fact]
    public async Task ExcelSource_WithVariousTypes_ReadsAllTypesCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithVariousTypes
                {
                    IntValue = 42,
                    LongValue = 1234567890L,
                    ShortValue = 100,
                    FloatValue = 3.14f,
                    DoubleValue = 2.5,
                    DecimalValue = 99.99m,
                    StringValue = "Test",
                    BoolValue = true,
                    DateTimeValue = new DateTime(2024, 1, 1),
                    GuidValue = Guid.Parse("550e8400-e29b-41d4-a716-446655440000"),
                },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithVariousTypes>(uri, resolver, config);
            IDataPipe<PocoWithVariousTypes> input = new StreamingDataPipe<PocoWithVariousTypes>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithVariousTypes>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithVariousTypes>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(1);
            results[0].IntValue.Should().Be(42);
            results[0].LongValue.Should().Be(1234567890L);
            results[0].ShortValue.Should().Be(100);
            results[0].DoubleValue.Should().Be(2.5);
            results[0].FloatValue.Should().Be(3.14f);
            results[0].DecimalValue.Should().Be(99.99m);
            results[0].StringValue.Should().Be("Test");
            results[0].BoolValue.Should().BeTrue();
            results[0].DateTimeValue.Should().Be(new DateTime(2024, 1, 1));
            results[0].GuidValue.Should().Be(new Guid("550e8400-e29b-41d4-a716-446655440000"));
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Backward Compatibility Tests

    [Fact]
    public async Task ExcelSource_WithExplicitRowMapper_WorksCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new SimplePoco { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new SimplePoco { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<SimplePoco>(uri, resolver, config);
            IDataPipe<SimplePoco> input = new StreamingDataPipe<SimplePoco>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read with explicit row mapper
            Func<ExcelRow, SimplePoco> rowMapper = row => new SimplePoco
            {
                Id = row.Get("id", 0),
                Name = row.Get("name", string.Empty) ?? string.Empty,
                CreatedAt = row.Get("createdat", default(DateTime)),
                IsActive = row.Get("isactive", false),
                Amount = row.Get("amount", 0m),
            };

            var source = new ExcelSourceNode<SimplePoco>(uri, rowMapper, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<SimplePoco>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Common Attributes Tests

    [Fact]
    public async Task ExcelSource_WithCommonColumnAttribute_ReadsDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithCommonAttributes { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new PocoWithCommonAttributes { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithCommonAttributes>(uri, resolver, config);
            IDataPipe<PocoWithCommonAttributes> input = new StreamingDataPipe<PocoWithCommonAttributes>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithCommonAttributes>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithCommonAttributes>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[0].CreatedAt.Should().Be(new DateTime(2024, 1, 1));
            results[0].IsActive.Should().BeTrue();
            results[0].Amount.Should().Be(100.50m);

            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
            results[1].CreatedAt.Should().Be(new DateTime(2024, 1, 2));
            results[1].IsActive.Should().BeFalse();
            results[1].Amount.Should().Be(200.75m);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task ExcelSink_WithCommonColumnAttribute_WritesDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithCommonAttributes { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new PocoWithCommonAttributes { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<PocoWithCommonAttributes>(uri, resolver, config);
            IDataPipe<PocoWithCommonAttributes> input = new StreamingDataPipe<PocoWithCommonAttributes>(data.ToAsyncEnumerable());

            // Act
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var source = new ExcelSourceNode<PocoWithCommonAttributes>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithCommonAttributes>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task ExcelSource_WithCommonIgnoreColumnAttribute_ExcludesIgnoredProperties()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithCommonIgnore { Id = 1, IgnoredProperty = "Should not appear", Name = "John Doe", AlsoIgnored = "Also should not appear" },
                new PocoWithCommonIgnore { Id = 2, IgnoredProperty = "Should not appear", Name = "Jane Doe", AlsoIgnored = "Also should not appear" },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithCommonIgnore>(uri, resolver, config);
            IDataPipe<PocoWithCommonIgnore> input = new StreamingDataPipe<PocoWithCommonIgnore>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithCommonIgnore>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithCommonIgnore>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[0].IgnoredProperty.Should().BeEmpty();
            results[0].AlsoIgnored.Should().BeEmpty();

            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
            results[1].IgnoredProperty.Should().BeEmpty();
            results[1].AlsoIgnored.Should().BeEmpty();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task ExcelSink_WithCommonIgnoreColumnAttribute_ExcludesIgnoredProperties()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithCommonIgnore { Id = 1, IgnoredProperty = "Should not appear", Name = "John Doe", AlsoIgnored = "Also should not appear" },
                new PocoWithCommonIgnore { Id = 2, IgnoredProperty = "Should not appear", Name = "Jane Doe", AlsoIgnored = "Also should not appear" },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<PocoWithCommonIgnore>(uri, resolver, config);
            IDataPipe<PocoWithCommonIgnore> input = new StreamingDataPipe<PocoWithCommonIgnore>(data.ToAsyncEnumerable());

            // Act
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var source = new ExcelSourceNode<PocoWithCommonIgnore>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithCommonIgnore>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[0].IgnoredProperty.Should().BeEmpty();
            results[0].AlsoIgnored.Should().BeEmpty();

            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
            results[1].IgnoredProperty.Should().BeEmpty();
            results[1].AlsoIgnored.Should().BeEmpty();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task ExcelSource_WithMixedAttributes_WorksCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithMixedAttributes { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new PocoWithMixedAttributes { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithMixedAttributes>(uri, resolver, config);
            IDataPipe<PocoWithMixedAttributes> input = new StreamingDataPipe<PocoWithMixedAttributes>(data.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithMixedAttributes>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithMixedAttributes>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[0].CreatedAt.Should().Be(new DateTime(2024, 1, 1));
            results[0].IsActive.Should().BeTrue();
            results[0].Amount.Should().Be(100.50m);

            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
            results[1].CreatedAt.Should().Be(new DateTime(2024, 1, 2));
            results[1].IsActive.Should().BeFalse();
            results[1].Amount.Should().Be(200.75m);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task ExcelSink_WithMixedAttributes_WritesDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithMixedAttributes { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new PocoWithMixedAttributes { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<PocoWithMixedAttributes>(uri, resolver, config);
            IDataPipe<PocoWithMixedAttributes> input = new StreamingDataPipe<PocoWithMixedAttributes>(data.ToAsyncEnumerable());

            // Act
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var source = new ExcelSourceNode<PocoWithMixedAttributes>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithMixedAttributes>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(1);
            results[0].Name.Should().Be("John Doe");
            results[1].Id.Should().Be(2);
            results[1].Name.Should().Be("Jane Doe");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Test Models

    private sealed class SimplePoco
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithAttributes
    {
        [Column("user_id")]
        public int Id { get; set; }

        [Column("full_name")]
        public string Name { get; set; } = string.Empty;

        [Column("created_date")]
        public DateTime CreatedAt { get; set; }

        [Column("is_active")]
        public bool IsActive { get; set; }

        [Column("total_amount")]
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithIgnore
    {
        public int Id { get; set; }

        [IgnoreColumn]
        public string IgnoredProperty { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        [Column("ignored", Ignore = true)]
        public string AlsoIgnored { get; set; } = string.Empty;
    }

    private sealed class PocoWithCommonAttributes
    {
        [Column("user_id")]
        public int Id { get; set; }

        [Column("full_name")]
        public string Name { get; set; } = string.Empty;

        [Column("created_date")]
        public DateTime CreatedAt { get; set; }

        [Column("is_active")]
        public bool IsActive { get; set; }

        [Column("total_amount")]
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithCommonIgnore
    {
        public int Id { get; set; }

        [IgnoreColumn]
        public string IgnoredProperty { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        [Column("ignored", Ignore = true)]
        public string AlsoIgnored { get; set; } = string.Empty;
    }

    private sealed class PocoWithMixedAttributes
    {
        [Column("user_id")]
        public int Id { get; set; }

        [Column("full_name")]
        public string Name { get; set; } = string.Empty;

        [Column("created_date")]
        public DateTime CreatedAt { get; set; }

        [Column("is_active")]
        public bool IsActive { get; set; }

        [Column("total_amount")]
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithNullableTypes
    {
        public int? Id { get; set; }
        public string? Name { get; set; }
        public DateTime? CreatedAt { get; set; }
        public bool? IsActive { get; set; }
        public decimal? Amount { get; set; }
    }

    private sealed class PocoWithMixedCase
    {
        public int UserId { get; set; }
        public string FullName { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class PocoWithVariousTypes
    {
        public int IntValue { get; set; }
        public long LongValue { get; set; }
        public short ShortValue { get; set; }
        public double DoubleValue { get; set; }
        public float FloatValue { get; set; }
        public decimal DecimalValue { get; set; }
        public string StringValue { get; set; } = string.Empty;
        public bool BoolValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public Guid GuidValue { get; set; }
    }

    #endregion

    #region Round-Trip Tests

    [Fact]
    public async Task Excel_RoundTrip_WithConventionMapping_PreservesData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var originalData = new[]
            {
                new SimplePoco { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new SimplePoco { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<SimplePoco>(uri, resolver, config);
            IDataPipe<SimplePoco> input = new StreamingDataPipe<SimplePoco>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<SimplePoco>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<SimplePoco>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(originalData[0].Id);
            results[0].Name.Should().Be(originalData[0].Name);
            results[0].CreatedAt.Should().Be(originalData[0].CreatedAt);
            results[0].IsActive.Should().Be(originalData[0].IsActive);
            results[0].Amount.Should().Be(originalData[0].Amount);

            results[1].Id.Should().Be(originalData[1].Id);
            results[1].Name.Should().Be(originalData[1].Name);
            results[1].CreatedAt.Should().Be(originalData[1].CreatedAt);
            results[1].IsActive.Should().Be(originalData[1].IsActive);
            results[1].Amount.Should().Be(originalData[1].Amount);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Excel_RoundTrip_WithAttributeMapping_PreservesData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            // Arrange
            var originalData = new[]
            {
                new PocoWithAttributes { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new PocoWithAttributes { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ExcelSinkNode<PocoWithAttributes>(uri, resolver, config);
            IDataPipe<PocoWithAttributes> input = new StreamingDataPipe<PocoWithAttributes>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new ExcelSourceNode<PocoWithAttributes>(uri, resolver, config);
            var outPipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = new List<PocoWithAttributes>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                results.Add(item);
            }

            // Assert
            results.Should().HaveCount(2);
            results[0].Id.Should().Be(originalData[0].Id);
            results[0].Name.Should().Be(originalData[0].Name);
            results[0].CreatedAt.Should().Be(originalData[0].CreatedAt);
            results[0].IsActive.Should().Be(originalData[0].IsActive);
            results[0].Amount.Should().Be(originalData[0].Amount);

            results[1].Id.Should().Be(originalData[1].Id);
            results[1].Name.Should().Be(originalData[1].Name);
            results[1].CreatedAt.Should().Be(originalData[1].CreatedAt);
            results[1].IsActive.Should().Be(originalData[1].IsActive);
            results[1].Amount.Should().Be(originalData[1].Amount);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion
}
