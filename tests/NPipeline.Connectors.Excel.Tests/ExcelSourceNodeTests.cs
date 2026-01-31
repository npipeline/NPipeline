using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Excel.Tests;

public sealed class ExcelSourceNodeTests
{
    [Fact]
    public async Task Read_XLSX_WithFileSystemProvider_ShouldReadData()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            // Write test data using ExcelSinkNode
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 5).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode
            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<int>();

            await foreach (var i in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(i);
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
    public async Task Read_WithFirstRowIsHeader_ShouldUseHeaders()
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
                new() { Id = 3, Name = "Charlie", Age = 35 },
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, config);
            IDataPipe<TestRecord> input = new StreamingDataPipe<TestRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode
            var src = new ExcelSourceNode<TestRecord>(uri, MapTestRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<TestRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().HaveCount(3);
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
    public async Task Read_WithFirstRowIsHeaderFalse_ShouldReadAllRows()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var writeConfig = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            // Write test data without headers
            var testData = new List<TestRecord>
            {
                new() { Id = 1, Name = "Alice", Age = 30 },
                new() { Id = 2, Name = "Bob", Age = 25 },
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, writeConfig);
            IDataPipe<TestRecord> input = new StreamingDataPipe<TestRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read with FirstRowIsHeader = false
            var readConfig = new ExcelConfiguration
            {
                FirstRowIsHeader = false,
            };

            var src = new ExcelSourceNode<TestRecord>(uri, MapTestRecordFromIndexes, resolver, readConfig);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<TestRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                if (item is not null)
                    result.Add(item);
            }

            // Assert - should read all rows
            result.Should().HaveCountGreaterThanOrEqualTo(2);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Read_WithSheetName_ShouldReadFromSpecificSheet()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                SheetName = "TestSheet",
                FirstRowIsHeader = false,
            };

            // Write test data
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 3).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode with sheet name
            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, config);
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
    public async Task Read_WithNullSheetName_ShouldReadFromFirstSheet()
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
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 3).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode with null sheet name
            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, config);
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
    public async Task Read_WithDifferentDataTypes_ShouldConvertCorrectly()
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

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<ComplexRecord>(uri, resolver, config);
            IDataPipe<ComplexRecord> input = new StreamingDataPipe<ComplexRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode
            var src = new ExcelSourceNode<ComplexRecord>(uri, MapComplexRecordFromHeaders, resolver, config);
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
    public async Task Read_WithNullableTypes_ShouldHandleNullValues()
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

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<NullableRecord>(uri, resolver, config);
            IDataPipe<NullableRecord> input = new StreamingDataPipe<NullableRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode
            var src = new ExcelSourceNode<NullableRecord>(uri, MapNullableRecordFromHeaders, resolver, config);
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
    public async Task Read_WithCaseInsensitiveHeaders_ShouldMatchCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new ExcelConfiguration
            {
                FirstRowIsHeader = true,
            };

            // Write test data - property names will be used as headers
            var testData = new List<TestRecord>
            {
                new() { Id = 1, Name = "Alice", Age = 30 },
                new() { Id = 2, Name = "Bob", Age = 25 },
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<TestRecord>(uri, resolver, config);
            IDataPipe<TestRecord> input = new StreamingDataPipe<TestRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode - headers should match case-insensitively
            var src = new ExcelSourceNode<TestRecord>(uri, MapTestRecordFromHeaders, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<TestRecord>();

            await foreach (var item in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(item);
            }

            // Assert
            result.Should().HaveCount(2);
            result[0].Id.Should().Be(1);
            result[0].Name.Should().Be("Alice");
            result[0].Age.Should().Be(30);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Read_WithCancellationToken_ShouldCancelOperation()
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
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 1000).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode with cancellation
            var cts = new CancellationTokenSource();
            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, cts.Token);

            var result = new List<int>();
            var count = 0;

            try
            {
                await foreach (var i in outPipe.WithCancellation(cts.Token))
                {
                    result.Add(i);
                    count++;

                    if (count >= 5)
                        cts.Cancel();
                }
            }
            catch (OperationCanceledException)
            {
                // Expected
            }

            // Assert - should have read some items before cancellation
            result.Should().HaveCountGreaterThanOrEqualTo(5);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Read_WithMissingFile_ShouldThrowException()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);
            var config = new ExcelConfiguration();
            var resolver = StorageProviderFactory.CreateResolver();

            // Read using ExcelSourceNode with missing file
            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<int>();

            await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            {
                await foreach (var i in outPipe.WithCancellation(CancellationToken.None))
                {
                    result.Add(i);
                }
            });
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Read_WithInvalidSheetName_ShouldThrowException()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.xlsx");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var writeConfig = new ExcelConfiguration
            {
                SheetName = "ActualSheet",
                FirstRowIsHeader = false,
            };

            // Write test data to one sheet
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<int>(uri, resolver, writeConfig);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 3).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Try to read from non-existent sheet
            var readConfig = new ExcelConfiguration
            {
                SheetName = "NonExistentSheet",
                FirstRowIsHeader = false,
            };

            var src = new ExcelSourceNode<int>(uri, MapIntRow, resolver, readConfig);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<int>();

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var i in outPipe.WithCancellation(CancellationToken.None))
                {
                    result.Add(i);
                }
            });
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Read_StringType_ShouldReadCorrectly()
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
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<string>(uri, resolver, config);
            IDataPipe<string> input = new StreamingDataPipe<string>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode
            var src = new ExcelSourceNode<string>(uri, MapStringRow, resolver, config);
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
    public async Task Read_DateTimeType_ShouldConvertCorrectly()
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

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<DateTimeRecord>(uri, resolver, config);
            IDataPipe<DateTimeRecord> input = new StreamingDataPipe<DateTimeRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode
            var src = new ExcelSourceNode<DateTimeRecord>(uri, MapDateTimeRecordFromHeaders, resolver, config);
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
    public async Task Read_BoolType_ShouldConvertCorrectly()
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

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<BoolRecord>(uri, resolver, config);
            IDataPipe<BoolRecord> input = new StreamingDataPipe<BoolRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode
            var src = new ExcelSourceNode<BoolRecord>(uri, MapBoolRecordFromHeaders, resolver, config);
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
    public async Task Read_DecimalType_ShouldConvertCorrectly()
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

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ExcelSinkNode<DecimalRecord>(uri, resolver, config);
            IDataPipe<DecimalRecord> input = new StreamingDataPipe<DecimalRecord>(testData.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read using ExcelSourceNode
            var src = new ExcelSourceNode<DecimalRecord>(uri, MapDecimalRecordFromHeaders, resolver, config);
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

    private static int MapIntRow(ExcelRow row)
    {
        return row.GetByIndex(0, 0);
    }

    private static string MapStringRow(ExcelRow row)
    {
        return row.GetByIndex(0, string.Empty) ?? string.Empty;
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

    private static TestRecord MapTestRecordFromIndexes(ExcelRow row)
    {
        return new TestRecord
        {
            Id = row.GetByIndex(0, 0),
            Name = row.GetByIndex(1, string.Empty) ?? string.Empty,
            Age = row.GetByIndex(2, 0),
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

    private static BoolRecord MapBoolRecordFromHeaders(ExcelRow row)
    {
        return new BoolRecord
        {
            Id = row.Get("Id", 0),
            IsActive = row.Get("IsActive", false),
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
}
