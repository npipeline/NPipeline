using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_PerformanceOptimization.Nodes;

/// <summary>
///     Source node that generates performance test data with varying complexity levels.
///     This node creates test data that demonstrates different optimization scenarios.
/// </summary>
public class PerformanceDataSource : SourceNode<PerformanceDataItem>
{
    private readonly int _itemCount;
    private readonly Random _random;

    /// <summary>
    ///     Initializes a new instance of the PerformanceDataSource.
    /// </summary>
    /// <param name="itemCount">The number of test items to generate. Defaults to 1000.</param>
    public PerformanceDataSource(int itemCount = 1000)
    {
        _itemCount = itemCount;
        _random = new Random(42); // Fixed seed for reproducible results
    }

    /// <summary>
    ///     Generates a collection of performance test data items.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated test items.</returns>
    public override IDataPipe<PerformanceDataItem> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Generating {_itemCount} performance test data items...");

        var items = new List<PerformanceDataItem>(_itemCount);

        // Generate items with varying complexity to test different optimization scenarios
        for (var i = 1; i <= _itemCount; i++)
        {
            var item = CreateTestItem(i);
            items.Add(item);

            // Report progress every 100 items
            if (i % 100 == 0)
                Console.WriteLine($"Generated {i}/{_itemCount} test items...");
        }

        Console.WriteLine($"Generated {items.Count} performance test data items");
        Console.WriteLine("Complexity distribution:");
        Console.WriteLine($"  Simple (1-3): {items.Count(x => x.ProcessingComplexity <= 3)} items");
        Console.WriteLine($"  Medium (4-7): {items.Count(x => x.ProcessingComplexity > 3 && x.ProcessingComplexity <= 7)} items");
        Console.WriteLine($"  Complex (8-10): {items.Count(x => x.ProcessingComplexity > 7)} items");
        Console.WriteLine();

        // Return a InMemoryDataPipe containing our test items
        return new InMemoryDataPipe<PerformanceDataItem>(items, "PerformanceDataSource");
    }

    /// <summary>
    ///     Creates a test item with appropriate characteristics for performance testing.
    /// </summary>
    private PerformanceDataItem CreateTestItem(int id)
    {
        // Create a balanced distribution of complexity levels
        int complexity;

        if (id % 3 == 0)
        {
            // 33% simple items (good for sync fast paths)
            complexity = _random.Next(1, 4);
        }
        else if (id % 3 == 1)
        {
            // 33% medium complexity items
            complexity = _random.Next(4, 8);
        }
        else
        {
            // 33% complex items (require async processing)
            complexity = _random.Next(8, 11);
        }

        // Determine if this item should use synchronous path
        var shouldUseSynchronousPath = complexity <= 3;

        // Randomly decide if this should use ValueTask for testing
        var shouldUseValueTask = _random.NextDouble() > 0.5;

        // Create data with varying size based on complexity
        var dataLength = 10 + complexity * 10 + _random.Next(0, 50);
        var data = GenerateTestData(dataLength, complexity);

        return new PerformanceDataItem
        {
            Id = id,
            Data = data,
            ProcessingComplexity = complexity,
            ShouldUseSynchronousPath = shouldUseSynchronousPath,
            ShouldUseValueTask = shouldUseValueTask,
        };
    }

    /// <summary>
    ///     Generates test data with characteristics based on complexity.
    /// </summary>
    private string GenerateTestData(int length, int complexity)
    {
        var chars = new char[length];

        // Generate different patterns based on complexity
        for (var i = 0; i < length; i++)
        {
            switch (complexity % 4)
            {
                case 0:
                    // Alphabetic characters
                    chars[i] = (char)('A' + i % 26);
                    break;
                case 1:
                    // Alphanumeric
                    chars[i] = i % 2 == 0
                        ? (char)('A' + i % 26)
                        : (char)('0' + i % 10);

                    break;
                case 2:
                    // Mixed case and numbers
                    chars[i] = i % 3 == 0
                        ? (char)('a' + i % 26)
                        : i % 3 == 1
                            ? (char)('A' + i % 26)
                            : (char)('0' + i % 10);

                    break;
                default:
                    // Special characters for high complexity
                    chars[i] = i % 5 == 0
                        ? (char)('!' + i % 10)
                        : (char)('A' + i % 52);

                    break;
            }
        }

        return new string(chars);
    }
}
