using AwesomeAssertions;
using NPipeline.Connectors.Excel;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Excel.Tests;

public sealed class ExcelRoundTripTests
{
    [Fact]
    public async Task RoundTrip_WithFileSystemProvider_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data
            var originalData = Enumerable.Range(1, 5).ToList();
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> writeInput = new StreamingDataPipe<int>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<int>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - data should be preserved
            readData.Should().Equal(originalData);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithHeaders_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data
            var originalData = new List<TestRecord>
            {
                new() { Id = 1, Name = "Alice", Age = 30 },
                new() { Id = 2, Name = "Bob", Age = 25 },
                new() { Id = 3, Name = "Charlie", Age = 35 },
            };

            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, config);
            IDataPipe<TestRecord> writeInput = new StreamingDataPipe<TestRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<TestRecord>(uri, MapTestRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<TestRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - data should be preserved
            readData.Should().HaveCount(3);
            readData[0].Id.Should().Be(1);
            readData[0].Name.Should().Be("Alice");
            readData[0].Age.Should().Be(30);
            readData[1].Id.Should().Be(2);
            readData[1].Name.Should().Be("Bob");
            readData[1].Age.Should().Be(25);
            readData[2].Id.Should().Be(3);
            readData[2].Name.Should().Be("Charlie");
            readData[2].Age.Should().Be(35);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithComplexTypes_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write complex data
            var originalData = new List<ComplexRecord>
            {
                new()
                {
                    Id = 1,
                    Name = "Test",
                    Age = 30,
                    Salary = 50000.50m,
                    IsActive = true,
                    BirthDate = new DateTime(1990, 1, 1),
                    Score = 95.5,
                    NullableValue = 10,
                },
                new()
                {
                    Id = 2,
                    Name = "Test2",
                    Age = 25,
                    Salary = 60000.75m,
                    IsActive = false,
                    BirthDate = new DateTime(1995, 5, 15),
                    Score = 87.3,
                    NullableValue = null,
                },
            };

            var sink = new ExcelSinkNode<ComplexRecord>(uri, resolver, config);
            IDataPipe<ComplexRecord> writeInput = new StreamingDataPipe<ComplexRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<ComplexRecord>(uri, MapComplexRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<ComplexRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - all complex data should be preserved
            readData.Should().HaveCount(2);

            // First record
            readData[0].Id.Should().Be(1);
            readData[0].Name.Should().Be("Test");
            readData[0].Age.Should().Be(30);
            readData[0].Salary.Should().Be(50000.50m);
            readData[0].IsActive.Should().BeTrue();
            readData[0].BirthDate.Should().Be(new DateTime(1990, 1, 1));
            readData[0].Score.Should().Be(95.5);
            readData[0].NullableValue.Should().Be(10);

            // Second record
            readData[1].Id.Should().Be(2);
            readData[1].Name.Should().Be("Test2");
            readData[1].Age.Should().Be(25);
            readData[1].Salary.Should().Be(60000.75m);
            readData[1].IsActive.Should().BeFalse();
            readData[1].BirthDate.Should().Be(new DateTime(1995, 5, 15));
            readData[1].Score.Should().Be(87.3);
            readData[1].NullableValue.Should().BeNull();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithMultipleDataTypes_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data with multiple types
            var originalData = new List<MixedTypeRecord>
            {
                new()
                {
                    IntValue = 42,
                    StringValue = "Hello",
                    DoubleValue = 3.14159,
                    BoolValue = true,
                    DateTimeValue = new DateTime(2020, 12, 25),
                    DecimalValue = 123.45m,
                },
                new()
                {
                    IntValue = -10,
                    StringValue = "World",
                    DoubleValue = -2.71828,
                    BoolValue = false,
                    DateTimeValue = new DateTime(2021, 6, 30),
                    DecimalValue = -999.99m,
                },
            };

            var sink = new ExcelSinkNode<MixedTypeRecord>(uri, resolver, config);
            IDataPipe<MixedTypeRecord> writeInput = new StreamingDataPipe<MixedTypeRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<MixedTypeRecord>(uri, MapMixedTypeRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<MixedTypeRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - all types should be preserved
            readData.Should().HaveCount(2);

            // First record
            readData[0].IntValue.Should().Be(42);
            readData[0].StringValue.Should().Be("Hello");
            readData[0].DoubleValue.Should().BeApproximately(3.14159, 0.00001);
            readData[0].BoolValue.Should().BeTrue();
            readData[0].DateTimeValue.Should().Be(new DateTime(2020, 12, 25));
            readData[0].DecimalValue.Should().Be(123.45m);

            // Second record
            readData[1].IntValue.Should().Be(-10);
            readData[1].StringValue.Should().Be("World");
            readData[1].DoubleValue.Should().BeApproximately(-2.71828, 0.00001);
            readData[1].BoolValue.Should().BeFalse();
            readData[1].DateTimeValue.Should().Be(new DateTime(2021, 6, 30));
            readData[1].DecimalValue.Should().Be(-999.99m);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithDifferentConfigurations_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var writeConfig = new ExcelConfiguration
            {
                SheetName = "TestSheet",
                FirstRowIsHeader = true,
                BufferSize = 8192,
            };

            var readConfig = new ExcelConfiguration
            {
                SheetName = "TestSheet",
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data with write configuration
            var originalData = new List<TestRecord>
            {
                new() { Id = 1, Name = "Alice", Age = 30 },
                new() { Id = 2, Name = "Bob", Age = 25 },
            };

            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, writeConfig);
            IDataPipe<TestRecord> writeInput = new StreamingDataPipe<TestRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back with read configuration
            var src = new ExcelSourceNode<TestRecord>(uri, MapTestRecordFromHeaders, resolver, readConfig);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<TestRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - data should be preserved regardless of configuration differences
            readData.Should().HaveCount(2);
            readData[0].Id.Should().Be(1);
            readData[0].Name.Should().Be("Alice");
            readData[0].Age.Should().Be(30);
            readData[1].Id.Should().Be(2);
            readData[1].Name.Should().Be("Bob");
            readData[1].Age.Should().Be(25);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithNullableTypes_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data with nullable values
            var originalData = new List<NullableRecord>
            {
                new() { Id = 1, NullableInt = 10, NullableString = "Test" },
                new() { Id = 2, NullableInt = null, NullableString = null },
                new() { Id = 3, NullableInt = 30, NullableString = "Test3" },
            };

            var sink = new ExcelSinkNode<NullableRecord>(uri, resolver, config);
            IDataPipe<NullableRecord> writeInput = new StreamingDataPipe<NullableRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<NullableRecord>(uri, MapNullableRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<NullableRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - nullable values should be preserved
            readData.Should().HaveCount(3);
            readData[0].NullableInt.Should().Be(10);
            readData[0].NullableString.Should().Be("Test");
            readData[1].NullableInt.Should().BeNull();
            readData[1].NullableString.Should().BeNull();
            readData[2].NullableInt.Should().Be(30);
            readData[2].NullableString.Should().Be("Test3");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithLargeDataset_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write large dataset
            var originalData = Enumerable.Range(1, 100).ToList();

            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> writeInput = new StreamingDataPipe<int>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<int>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - all data should be preserved
            readData.Should().HaveCount(100);
            readData.Should().Equal(originalData);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithEmptyData_ShouldCreateValidFile()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write empty data
            var originalData = new List<TestRecord>();

            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, config);
            IDataPipe<TestRecord> writeInput = new StreamingDataPipe<TestRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<TestRecord>(uri, MapTestRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<TestRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - should have no data
            readData.Should().BeEmpty();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithSpecialCharacters_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data with special characters
            var originalData = new List<TestRecord>
            {
                new() { Id = 1, Name = "Hello, World!", Age = 30 },
                new() { Id = 2, Name = "Test\tTab", Age = 25 },
                new() { Id = 3, Name = "Line1\nLine2", Age = 35 },
            };

            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, config);
            IDataPipe<TestRecord> writeInput = new StreamingDataPipe<TestRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<TestRecord>(uri, MapTestRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<TestRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - special characters should be preserved
            readData.Should().HaveCount(3);
            readData[0].Name.Should().Be("Hello, World!");
            readData[1].Name.Should().Be("Test\tTab");
            readData[2].Name.Should().Be("Line1\nLine2");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithCustomSheetName_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                SheetName = "MyCustomSheet",
                FirstRowIsHeader = false,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data to custom sheet
            var originalData = Enumerable.Range(1, 5).ToList();

            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> writeInput = new StreamingDataPipe<int>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back from custom sheet
            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<int>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - data should be preserved
            readData.Should().Equal(originalData);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithDateTimePrecision_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data with precise DateTime values
            var originalData = new List<DateTimeRecord>
            {
                new() { Id = 1, Date = new DateTime(2020, 1, 1, 12, 30, 45) },
                new() { Id = 2, Date = new DateTime(2021, 6, 15, 8, 15, 30) },
            };

            var sink = new ExcelSinkNode<DateTimeRecord>(uri, resolver, config);
            IDataPipe<DateTimeRecord> writeInput = new StreamingDataPipe<DateTimeRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<DateTimeRecord>(uri, MapDateTimeRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<DateTimeRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - DateTime values should be preserved (within Excel precision)
            readData.Should().HaveCount(2);
            readData[0].Date.Should().BeCloseTo(new DateTime(2020, 1, 1, 12, 30, 45), TimeSpan.FromSeconds(1));
            readData[1].Date.Should().BeCloseTo(new DateTime(2021, 6, 15, 8, 15, 30), TimeSpan.FromSeconds(1));
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithDecimalPrecision_ShouldPreserveData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();

            // Write data with precise decimal values
            var originalData = new List<DecimalRecord>
            {
                new() { Id = 1, Amount = 1234.5678m },
                new() { Id = 2, Amount = 9876.5432m },
            };

            var sink = new ExcelSinkNode<DecimalRecord>(uri, resolver, config);
            IDataPipe<DecimalRecord> writeInput = new StreamingDataPipe<DecimalRecord>(originalData.ToAsyncEnumerable());
            await sink.ExecuteAsync(writeInput, PipelineContext.Default, CancellationToken.None);

            // Read data back
            var src = new ExcelSourceNode<DecimalRecord>(uri, MapDecimalRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var readData = new List<DecimalRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                readData.Add(item);
            }

            // Assert - decimal values should be preserved (within Excel precision)
            readData.Should().HaveCount(2);
            readData[0].Amount.Should().BeApproximately(1234.5678m, 0.0001m);
            readData[1].Amount.Should().BeApproximately(9876.5432m, 0.0001m);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    private static int MapIntRow(ExcelRow row)
    {
        return row.GetByIndex(0, 0);
    }

    private static TestRecord MapTestRecordFromHeaders(ExcelRow row)
    {
        return new TestRecord
        {
            Id = row.Get("Id", 0),
            Name = row.Get("Name", string.Empty) ?? string.Empty,
            Age = row.Get("Age", 0),
        };
    }

    private static ComplexRecord MapComplexRecordFromHeaders(ExcelRow row)
    {
        return new ComplexRecord
        {
            Id = row.Get("Id", 0),
            Name = row.Get("Name", string.Empty) ?? string.Empty,
            Age = row.Get("Age", 0),
            Salary = row.Get("Salary", 0m),
            IsActive = row.Get("IsActive", false),
            BirthDate = row.Get("BirthDate", default(DateTime)),
            Score = row.Get("Score", 0d),
            NullableValue = row.Get<int?>("NullableValue"),
        };
    }

    private static MixedTypeRecord MapMixedTypeRecordFromHeaders(ExcelRow row)
    {
        return new MixedTypeRecord
        {
            IntValue = row.Get("IntValue", 0),
            StringValue = row.Get("StringValue", string.Empty) ?? string.Empty,
            DoubleValue = row.Get("DoubleValue", 0d),
            BoolValue = row.Get("BoolValue", false),
            DateTimeValue = row.Get("DateTimeValue", default(DateTime)),
            DecimalValue = row.Get("DecimalValue", 0m),
        };
    }

    private static NullableRecord MapNullableRecordFromHeaders(ExcelRow row)
    {
        return new NullableRecord
        {
            Id = row.Get("Id", 0),
            NullableInt = row.Get<int?>("NullableInt"),
            NullableString = row.Get<string>("NullableString"),
        };
    }

    private static DateTimeRecord MapDateTimeRecordFromHeaders(ExcelRow row)
    {
        return new DateTimeRecord
        {
            Id = row.Get("Id", 0),
            Date = row.Get("Date", default(DateTime)),
        };
    }

    private static DecimalRecord MapDecimalRecordFromHeaders(ExcelRow row)
    {
        return new DecimalRecord
        {
            Id = row.Get("Id", 0),
            Amount = row.Get("Amount", 0m),
        };
    }

    // Test record classes
    private sealed record TestRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    private sealed record ComplexRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public decimal Salary { get; set; }
        public bool IsActive { get; set; }
        public DateTime BirthDate { get; set; }
        public double Score { get; set; }
        public int? NullableValue { get; set; }
    }

    private sealed record MixedTypeRecord
    {
        public int IntValue { get; set; }
        public string StringValue { get; set; } = string.Empty;
        public double DoubleValue { get; set; }
        public bool BoolValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public decimal DecimalValue { get; set; }
    }

    private sealed record NullableRecord
    {
        public int Id { get; set; }
        public int? NullableInt { get; set; }
        public string? NullableString { get; set; }
    }

    private sealed record DateTimeRecord
    {
        public int Id { get; set; }
        public DateTime Date { get; set; }
    }

    private sealed record DecimalRecord
    {
        public int Id { get; set; }
        public decimal Amount { get; set; }
    }
}
