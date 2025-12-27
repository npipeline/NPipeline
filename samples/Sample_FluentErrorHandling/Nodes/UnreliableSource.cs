using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_BasicErrorHandling.Nodes;

/// <summary>
///     Source node that generates string data but simulates intermittent failures.
///     This node demonstrates how to handle unreliable data sources by randomly throwing
///     exceptions to simulate real-world failure scenarios.
/// </summary>
public class UnreliableSource : SourceNode<string>
{
    private readonly Random _random = new();
    private double _failureRate = 0.3; // 30% chance of failure by default

    /// <summary>
    ///     Initializes a new instance of the UnreliableSource class.
    ///     Parameterless constructor for DI compatibility.
    /// </summary>
    public UnreliableSource()
    {
    }

    /// <summary>
    ///     Generates a collection of messages with simulated intermittent failures.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated messages.</returns>
    /// <exception cref="InvalidOperationException">Thrown randomly to simulate source failures.</exception>
    public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("UnreliableSource: Starting to generate messages");

        // Check if we should simulate a failure
        if (ShouldSimulateFailure())
        {
            Console.WriteLine("UnreliableSource: Simulating source failure!");
            throw new InvalidOperationException("Simulated source failure: Unable to connect to data source");
        }

        // Generate a list of messages
        var messages = new List<string>();

        for (var i = 1; i <= 10; i++)
        {
            // Randomly decide if this specific message should fail
            if (ShouldSimulateFailure())
            {
                Console.WriteLine($"UnreliableSource: Failed to generate message #{i}");
                throw new InvalidOperationException($"Simulated failure: Unable to generate message #{i}");
            }

            messages.Add($"Message #{i} from unreliable source");
        }

        Console.WriteLine($"UnreliableSource: Successfully generated {messages.Count} messages");

        // Return a InMemoryDataPipe containing our messages
        return new InMemoryDataPipe<string>(messages, "UnreliableSource");
    }

    /// <summary>
    ///     Determines whether to simulate a failure based on the failure rate.
    /// </summary>
    /// <returns>True if a failure should be simulated, false otherwise.</returns>
    private bool ShouldSimulateFailure()
    {
        return _random.NextDouble() < _failureRate;
    }

    /// <summary>
    ///     Sets the failure rate for testing purposes.
    ///     A value between 0.0 (never fails) and 1.0 (always fails).
    /// </summary>
    /// <param name="failureRate">The failure rate to set.</param>
    public void SetFailureRate(double failureRate)
    {
        if (failureRate < 0.0 || failureRate > 1.0)
            throw new ArgumentOutOfRangeException(nameof(failureRate), "Failure rate must be between 0.0 and 1.0");

        _failureRate = failureRate;
        Console.WriteLine($"UnreliableSource: Failure rate set to {_failureRate:P1}");
    }
}
