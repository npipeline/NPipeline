using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_BasicPipeline.Nodes;

/// <summary>
///     Sink node that outputs processed strings to the console.
///     This node demonstrates how to create a simple sink that consumes string data
///     by inheriting directly from SinkNode&lt;string&gt;.
/// </summary>
public class ConsoleSink : SinkNode<string>
{
    /// <summary>
    ///     Processes the input strings by writing them to the console with formatting.
    /// </summary>
    /// <param name="input">The data pipe containing input strings to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting to process messages in ConsoleSink");

        var messageCount = 0;

        // Use await foreach to consume all messages from the input pipe
        // This is the standard pattern for consuming data in sink nodes
        await foreach (var message in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            messageCount++;
            Console.WriteLine($"Processing message #{messageCount}: {message}");
            Console.WriteLine($"[Pipeline Output] {message}");
        }

        Console.WriteLine($"ConsoleSink processed {messageCount} messages");
    }
}
