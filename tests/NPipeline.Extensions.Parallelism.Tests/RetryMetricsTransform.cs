// ReSharper disable ClassNeverInstantiated.Global

using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tests.Common;

namespace NPipeline.Extensions.Parallelism.Tests;

public sealed class RetryMetricsTransform : TransformNode<int, int>
{
    public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
    {
        var count = SharedTestState.AttemptCounts.AddOrUpdate(item, 1, (_, c) => c + 1);

        if (count < 3)
            throw new InvalidOperationException("forced failure for retry");

        return Task.FromResult(item * 2);
    }
}
