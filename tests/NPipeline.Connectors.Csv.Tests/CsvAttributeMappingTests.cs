using System.Globalization;
using AwesomeAssertions;
using NPipeline.Connectors.Csv.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Csv.Tests;

/// <summary>
///     Integration tests for attribute-based mapping with CsvSource and CsvSink nodes.
///     Validates end-to-end functionality of the attribute mapping feature.
/// </summary>
public sealed class CsvAttributeMappingTests
{
    #region Case-Insensitive Column Matching Tests

    [Fact]
    public async Task CsvSource_WithDifferentColumnCase_MapsSuccessfully()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var csvContent = "userid,fullname,createdat,isactive\n1,John Doe,2024-01-01,true\n2,Jane Doe,2024-01-02,false\n";
            await File.WriteAllTextAsync(tempFile, csvContent);

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new CsvSourceNode<PocoWithMixedCase>(uri, resolver, config);

            // Act
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

    #region Backward Compatibility Tests

    [Fact]
    public async Task CsvSource_WithExplicitRowMapper_WorksCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var csvContent = "id,name\n1,John Doe\n2,Jane Doe\n";
            await File.WriteAllTextAsync(tempFile, csvContent);

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Use explicit row mapper
            Func<CsvRow, SimplePoco> rowMapper = row => new SimplePoco
            {
                Id = row.Get("id", 0),
                Name = row.Get("name", string.Empty),
            };

            var source = new CsvSourceNode<SimplePoco>(uri, rowMapper, resolver, config);

            // Act
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
        [CsvColumn("user_id")]
        public int Id { get; set; }

        [CsvColumn("full_name")]
        public string Name { get; set; } = string.Empty;

        [CsvColumn("created_date")]
        public DateTime CreatedAt { get; set; }

        [CsvColumn("is_active")]
        public bool IsActive { get; set; }

        [CsvColumn("total_amount")]
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithIgnore
    {
        public int Id { get; set; }

        [CsvIgnore]
        public string IgnoredProperty { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        [CsvColumn("ignored", Ignore = true)]
        public string AlsoIgnored { get; set; } = string.Empty;
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

        public string FullNameSetter
        {
            get => FullName;
            set => FullName = value;
        }
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

    #region Convention-Based Mapping Tests

    [Fact]
    public async Task CsvSource_WithConventionMapping_ReadsDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var csvContent = "id,name,createdat,isactive,amount\n1,John Doe,2024-01-01,true,100.50\n2,Jane Doe,2024-01-02,false,200.75\n";
            await File.WriteAllTextAsync(tempFile, csvContent);

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new CsvSourceNode<SimplePoco>(uri, resolver, config);

            // Act
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

    [Fact]
    public async Task CsvSink_WithConventionMapping_WritesDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var data = new[]
            {
                new SimplePoco { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new SimplePoco { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new CsvSinkNode<SimplePoco>(uri, resolver, config);
            IDataPipe<SimplePoco> input = new StreamingDataPipe<SimplePoco>(data.ToAsyncEnumerable());

            // Act
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Assert
            var lines = await File.ReadAllLinesAsync(tempFile);
            lines.Should().HaveCount(3); // Header + 2 data rows
            lines[0].Should().Be("id,name,createdat,isactive,amount");
            lines[1].Should().Contain("1");
            lines[1].Should().Contain("John Doe");
            lines[2].Should().Contain("2");
            lines[2].Should().Contain("Jane Doe");
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
    public async Task CsvSource_WithAttributeMapping_ReadsDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var csvContent = "user_id,full_name,created_date,is_active,total_amount\n1,John Doe,2024-01-01,true,100.50\n2,Jane Doe,2024-01-02,false,200.75\n";
            await File.WriteAllTextAsync(tempFile, csvContent);

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new CsvSourceNode<PocoWithAttributes>(uri, resolver, config);

            // Act
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

    [Fact]
    public async Task CsvSink_WithAttributeMapping_WritesDataCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithAttributes { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new PocoWithAttributes { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new CsvSinkNode<PocoWithAttributes>(uri, resolver, config);
            IDataPipe<PocoWithAttributes> input = new StreamingDataPipe<PocoWithAttributes>(data.ToAsyncEnumerable());

            // Act
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Assert
            var lines = await File.ReadAllLinesAsync(tempFile);
            lines.Should().HaveCount(3); // Header + 2 data rows
            lines[0].Should().Be("user_id,full_name,created_date,is_active,total_amount");
            lines[1].Should().Contain("1");
            lines[1].Should().Contain("John Doe");
            lines[2].Should().Contain("2");
            lines[2].Should().Contain("Jane Doe");
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
    public async Task CsvSource_WithIgnoreAttribute_ExcludesIgnoredProperties()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var csvContent = "id,name\n1,John Doe\n2,Jane Doe\n";
            await File.WriteAllTextAsync(tempFile, csvContent);

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new CsvSourceNode<PocoWithIgnore>(uri, resolver, config);

            // Act
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

    [Fact]
    public async Task CsvSink_WithIgnoreAttribute_ExcludesIgnoredProperties()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithIgnore { Id = 1, IgnoredProperty = "Should not appear", Name = "John Doe", AlsoIgnored = "Also should not appear" },
                new PocoWithIgnore { Id = 2, IgnoredProperty = "Should not appear", Name = "Jane Doe", AlsoIgnored = "Also should not appear" },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new CsvSinkNode<PocoWithIgnore>(uri, resolver, config);
            IDataPipe<PocoWithIgnore> input = new StreamingDataPipe<PocoWithIgnore>(data.ToAsyncEnumerable());

            // Act
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Assert
            var lines = await File.ReadAllLinesAsync(tempFile);
            lines.Should().HaveCount(3); // Header + 2 data rows
            lines[0].Should().Be("id,name");
            lines[1].Should().Be("1,John Doe");
            lines[2].Should().Be("2,Jane Doe");
            lines[1].Should().NotContain("Should not appear");
            lines[2].Should().NotContain("Should not appear");
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
    public async Task CsvSource_WithNullableTypes_HandlesNullValues()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var csvContent = "id,name,createdat,isactive,amount\n1,John,,true,\n2,Jane,2024-01-02,false,200.75\n";
            await File.WriteAllTextAsync(tempFile, csvContent);

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new CsvSourceNode<PocoWithNullableTypes>(uri, resolver, config);

            // Act
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

    [Fact]
    public async Task CsvSink_WithNullableTypes_WritesNullValues()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var data = new[]
            {
                new PocoWithNullableTypes { Id = 1, Name = "John", CreatedAt = null, IsActive = true, Amount = null },
                new PocoWithNullableTypes { Id = 2, Name = "Jane", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new CsvSinkNode<PocoWithNullableTypes>(uri, resolver, config);
            IDataPipe<PocoWithNullableTypes> input = new StreamingDataPipe<PocoWithNullableTypes>(data.ToAsyncEnumerable());

            // Act
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Assert
            var lines = await File.ReadAllLinesAsync(tempFile);
            lines.Should().HaveCount(3); // Header + 2 data rows
            lines[0].Should().Be("id,name,createdat,isactive,amount");
            lines[1].Should().Be("1,John,,True,");
            lines[2].Should().Contain("2,Jane");
            lines[2].Should().Contain("False");
            lines[2].Should().Contain("200.75");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    #endregion

    #region Round-Trip Tests

    [Fact]
    public async Task Csv_RoundTrip_WithConventionMapping_PreservesData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var originalData = new[]
            {
                new SimplePoco { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new SimplePoco { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new CsvSinkNode<SimplePoco>(uri, resolver, config);
            IDataPipe<SimplePoco> input = new StreamingDataPipe<SimplePoco>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new CsvSourceNode<SimplePoco>(uri, resolver, config);
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
    public async Task Csv_RoundTrip_WithAttributeMapping_PreservesData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var originalData = new[]
            {
                new PocoWithAttributes { Id = 1, Name = "John Doe", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100.50m },
                new PocoWithAttributes { Id = 2, Name = "Jane Doe", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200.75m },
            };

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new CsvSinkNode<PocoWithAttributes>(uri, resolver, config);
            IDataPipe<PocoWithAttributes> input = new StreamingDataPipe<PocoWithAttributes>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read
            var source = new CsvSourceNode<PocoWithAttributes>(uri, resolver, config);
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

    #region Various Types Tests

    [Fact]
    public async Task CsvSource_WithVariousTypes_ReadsAllTypesCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            // Arrange
            var csvContent =
                "intvalue,longvalue,shortvalue,doublevalue,floatvalue,decimalvalue,stringvalue,boolvalue,datetimevalue,guidvalue\n42,1234567890,100,3.14,2.5,99.99,Test,true,2024-01-01,550e8400-e29b-41d4-a716-446655440000\n";

            await File.WriteAllTextAsync(tempFile, csvContent);

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new CsvSourceNode<PocoWithVariousTypes>(uri, resolver, config);

            // Act
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
            results[0].DoubleValue.Should().Be(3.14);
            results[0].FloatValue.Should().Be(2.5f);
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

    [Fact]
    public async Task CsvSink_WithVariousTypes_WritesAllTypesCorrectly()
    {
        // Arrange
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");
        var uri = StorageUri.FromFilePath(tempFile);

        var poco = new PocoWithVariousTypes
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
        };

        var resolver = StorageProviderFactory.CreateResolver();
        var sink = new CsvSinkNode<PocoWithVariousTypes>(uri, resolver);
        IDataPipe<PocoWithVariousTypes> input = new StreamingDataPipe<PocoWithVariousTypes>(new[] { poco }.ToAsyncEnumerable());

        // Act
        await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

        var lines = await File.ReadAllLinesAsync(tempFile);

        // Assert
        lines.Should().HaveCount(2);
        lines[0].Should().Contain("intvalue");
        lines[0].Should().Contain("longvalue");
        lines[0].Should().Contain("shortvalue");
        lines[0].Should().Contain("floatvalue");
        lines[0].Should().Contain("doublevalue");
        lines[0].Should().Contain("decimalvalue");
        lines[0].Should().Contain("stringvalue");
        lines[0].Should().Contain("boolvalue");
        lines[0].Should().Contain("datetimevalue");
        lines[0].Should().Contain("guidvalue");
        lines[1].Should().Contain("42");
        lines[1].Should().Contain("1234567890");
        lines[1].Should().Contain("100");
        lines[1].Should().Contain("3.14");
        lines[1].Should().Contain("2.5");
        lines[1].Should().Contain("99.99");
        lines[1].Should().Contain("Test");
        lines[1].Should().Contain("True");
        lines[1].Should().Contain("01/01/2024 00:00:00");
        lines[1].Should().Contain("550e8400-e29b-41d4-a716-446655440000");

        // Cleanup
        File.Delete(tempFile);
    }

    #endregion
}
