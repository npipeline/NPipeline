using AwesomeAssertions;
using NPipeline.Connectors.Excel;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Tests.Excel;

public sealed class ExcelSinkNodeTests
{
    [Fact]
    public async Task Write_XLSX_WithFileSystemProvider_ShouldWriteData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            // Write test data
            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 5).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Verify file was created
            File.Exists(tempFile).Should().BeTrue();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithFirstRowIsHeader_ShouldWriteHeaderRow()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write test data with headers
            var testData = new List<TestRecord>
            {
                new() { Id = 1, Name = "Alice", Age = 30 },
                new() { Id = 2, Name = "Bob", Age = 25 },
            };

            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, config);
            IDataPipe<TestRecord> input = new StreamingDataPipe<TestRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify headers
            var src = new ExcelSourceNode<TestRecord>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<TestRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert - data should be correctly mapped
            result.Should().HaveCount(2);
            result[0].Id.Should().Be(1);
            result[0].Name.Should().Be("Alice");
            result[0].Age.Should().Be(30);
            result[1].Id.Should().Be(2);
            result[1].Name.Should().Be("Bob");
            result[1].Age.Should().Be(25);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithFirstRowIsHeaderFalse_ShouldNotWriteHeaderRow()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            // Write test data without headers
            var testData = new List<TestRecord>
            {
                new() { Id = 1, Name = "Alice", Age = 30 },
                new() { Id = 2, Name = "Bob", Age = 25 },
            };

            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, config);
            IDataPipe<TestRecord> input = new StreamingDataPipe<TestRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Verify file was created
            File.Exists(tempFile).Should().BeTrue();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithSheetName_ShouldCreateSheetWithName()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                SheetName = "CustomSheet",
                FirstRowIsHeader = false,
            };

            // Write test data
            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 3).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify sheet name
            var src = new ExcelSourceNode<int>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<int>();

            await foreach (var i in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(i);
            }

            // Assert
            result.Should().Equal(1, 2, 3);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithNullSheetName_ShouldCreateDefaultSheet()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                SheetName = null,
                FirstRowIsHeader = false,
            };

            // Write test data
            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 3).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<int>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<int>();

            await foreach (var i in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(i);
            }

            // Assert
            result.Should().Equal(1, 2, 3);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithDifferentDataTypes_ShouldWriteCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write test data with various types
            var testData = new List<ComplexRecord>
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

            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<ComplexRecord>(uri, resolver, config);
            IDataPipe<ComplexRecord> input = new StreamingDataPipe<ComplexRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<ComplexRecord>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<ComplexRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().HaveCount(2);
            result[0].Id.Should().Be(1);
            result[0].Name.Should().Be("Test");
            result[0].Age.Should().Be(30);
            result[0].Salary.Should().Be(50000.50m);
            result[0].IsActive.Should().BeTrue();
            result[0].BirthDate.Should().Be(new DateTime(1990, 1, 1));
            result[0].Score.Should().Be(95.5);
            result[0].NullableValue.Should().Be(10);

            result[1].Id.Should().Be(2);
            result[1].Name.Should().Be("Test2");
            result[1].Age.Should().Be(25);
            result[1].Salary.Should().Be(60000.75m);
            result[1].IsActive.Should().BeFalse();
            result[1].BirthDate.Should().Be(new DateTime(1995, 5, 15));
            result[1].Score.Should().Be(87.3);
            result[1].NullableValue.Should().BeNull();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithNullableTypes_ShouldHandleNullValues()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write test data with nullable values
            var testData = new List<NullableRecord>
            {
                new() { Id = 1, NullableInt = 10, NullableString = "Test" },
                new() { Id = 2, NullableInt = null, NullableString = null },
                new() { Id = 3, NullableInt = 30, NullableString = "Test3" },
            };

            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<NullableRecord>(uri, resolver, config);
            IDataPipe<NullableRecord> input = new StreamingDataPipe<NullableRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<NullableRecord>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<NullableRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().HaveCount(3);
            result[0].NullableInt.Should().Be(10);
            result[0].NullableString.Should().Be("Test");
            result[1].NullableInt.Should().BeNull();
            result[1].NullableString.Should().BeNull();
            result[2].NullableInt.Should().Be(30);
            result[2].NullableString.Should().Be("Test3");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithCancellationToken_ShouldCancelOperation()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            var cts = new CancellationTokenSource();
            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<int>(uri, resolver, config);

            // Create a slow input stream
            async IAsyncEnumerable<int> SlowInput()
            {
                for (var i = 1; i <= 1000; i++)
                {
                    await Task.Delay(10);
                    yield return i;
                }
            }

            IDataPipe<int> input = new StreamingDataPipe<int>(SlowInput());

            // Cancel after a short delay
            cts.CancelAfter(100);

            await Assert.ThrowsAsync<OperationCanceledException>(async () => { await sink.ExecuteAsync(input, PipelineContext.Default, cts.Token); });
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_StringType_ShouldWriteCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            // Write test data
            var testData = new List<string> { "Apple", "Banana", "Cherry" };
            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<string>(uri, resolver, config);
            IDataPipe<string> input = new StreamingDataPipe<string>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<string>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<string>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().Equal("Apple", "Banana", "Cherry");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_DateTimeType_ShouldWriteCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write test data
            var testData = new List<DateTimeRecord>
            {
                new() { Id = 1, Date = new DateTime(2020, 1, 1) },
                new() { Id = 2, Date = new DateTime(2021, 6, 15) },
            };

            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<DateTimeRecord>(uri, resolver, config);
            IDataPipe<DateTimeRecord> input = new StreamingDataPipe<DateTimeRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<DateTimeRecord>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<DateTimeRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().HaveCount(2);
            result[0].Date.Should().Be(new DateTime(2020, 1, 1));
            result[1].Date.Should().Be(new DateTime(2021, 6, 15));
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_BoolType_ShouldWriteCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write test data
            var testData = new List<BoolRecord>
            {
                new() { Id = 1, IsActive = true },
                new() { Id = 2, IsActive = false },
                new() { Id = 3, IsActive = true },
            };

            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<BoolRecord>(uri, resolver, config);
            IDataPipe<BoolRecord> input = new StreamingDataPipe<BoolRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<BoolRecord>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<BoolRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().HaveCount(3);
            result[0].IsActive.Should().BeTrue();
            result[1].IsActive.Should().BeFalse();
            result[2].IsActive.Should().BeTrue();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_DecimalType_ShouldWriteCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write test data
            var testData = new List<DecimalRecord>
            {
                new() { Id = 1, Amount = 1234.56m },
                new() { Id = 2, Amount = 7890.12m },
            };

            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<DecimalRecord>(uri, resolver, config);
            IDataPipe<DecimalRecord> input = new StreamingDataPipe<DecimalRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<DecimalRecord>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<DecimalRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().HaveCount(2);
            result[0].Amount.Should().Be(1234.56m);
            result[1].Amount.Should().Be(7890.12m);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_IntType_ShouldWriteCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            // Write test data
            var testData = new List<int> { 1, 2, 3, 4, 5 };
            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<int>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<int>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().Equal(1, 2, 3, 4, 5);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_DoubleType_ShouldWriteCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write test data
            var testData = new List<DoubleRecord>
            {
                new() { Id = 1, Value = 123.456 },
                new() { Id = 2, Value = 789.012 },
            };

            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<DoubleRecord>(uri, resolver, config);
            IDataPipe<DoubleRecord> input = new StreamingDataPipe<DoubleRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read back to verify
            var src = new ExcelSourceNode<DoubleRecord>(uri, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<DoubleRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().HaveCount(2);
            result[0].Value.Should().BeApproximately(123.456, 0.001);
            result[1].Value.Should().BeApproximately(789.012, 0.001);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Write_EmptyData_ShouldCreateValidFile()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write empty data
            var testData = new List<TestRecord>();
            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, config);
            IDataPipe<TestRecord> input = new StreamingDataPipe<TestRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Verify file was created
            File.Exists(tempFile).Should().BeTrue();
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
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

    private sealed record BoolRecord
    {
        public int Id { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed record DecimalRecord
    {
        public int Id { get; set; }
        public decimal Amount { get; set; }
    }

    private sealed record DoubleRecord
    {
        public int Id { get; set; }
        public double Value { get; set; }
    }
}
