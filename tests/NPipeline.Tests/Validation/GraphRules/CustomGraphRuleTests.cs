using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Extensions.Testing;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Validation.GraphRules;

public sealed class CustomGraphRuleTests
{
    [Fact]
    public void CustomRule_Should_Emit_Warning()
    {
        var builder = new PipelineBuilder()
            .WithValidationRule(new AlwaysWarnRule());

        builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "s", [1]);
        var ok = builder.TryBuild(out var pipeline, out var result);
        ok.Should().BeTrue();
        result.Issues.Should().Contain(i => i.Category == "Custom" && i.Severity == ValidationSeverity.Warning);
    }

    private sealed class AlwaysWarnRule : IGraphRule
    {
        public string Name => "AlwaysWarn";
        public bool StopOnError => false;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            if (context.Graph.Nodes.Count > 0)
                yield return new ValidationIssue(ValidationSeverity.Warning, "Custom rule executed", "Custom");
        }
    }


    private sealed class MiniPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var s = b.AddInMemorySourceWithDataFromContext(c, "s", [1]);
            var t = b.AddTransform<Passthrough, int, int>("t");
            var sink = b.AddInMemorySink<int>("sink");
            b.Connect(s, t);
            b.Connect(t, sink);
        }
    }

    private sealed class Passthrough : ITransformNode<int, int>
    {
        public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();
        public INodeErrorHandler? ErrorHandler { get; set; }

        public Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
