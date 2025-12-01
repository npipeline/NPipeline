using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_BasicPipeline.Nodes;

/// <summary>
///     Source node that generates "Hello World" messages for the basic pipeline.
///     This node demonstrates how to create a simple source that produces string data
///     by inheriting directly from SourceNode&lt;string&gt;.
/// </summary>
public class HelloWorldSource : SourceNode<string>
{
    /// <summary>
    ///     Generates a collection of "Hello World" messages with variations.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated messages.</returns>
    public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Generating Hello World messages");

        // Generate a list of Hello World messages with variations
        var messages = new List<string>
        {
            "Hello World",
            "Hello NPipeline",
            "Hello Samples",
            "Hello Pipeline Processing",
            "Hello Async Programming",
        };

        // Add some numbered variations for more interesting output
        for (var i = 1; i <= 5; i++)
        {
            messages.Add($"Hello World #{i}");
        }

        Console.WriteLine($"Generated {messages.Count} Hello World messages");

        // Return a InMemoryDataPipe containing our messages
        // Normally a source should generate data asynchronously, but for simplicity we return all at once here using InMemoryDataPipe
        return new InMemoryDataPipe<string>(messages, "HelloWorldSource");
    }
}
