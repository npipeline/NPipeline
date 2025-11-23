using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_CustomMergeNode.Infrastructure;
using Sample_CustomMergeNode.Models;
using Sample_CustomMergeNode.Strategies;

namespace Sample_CustomMergeNode.Nodes;

/// <summary>
///     Priority-based merge node for market data with advanced conflict resolution
/// </summary>
public class PriorityBasedMergeNode : CustomMergeNode<MarketDataTick>, ITransformNode<MarketDataTick, MarketDataTick>
{
    private readonly ILogger<PriorityBasedMergeNode>? _logger;
    private readonly PriorityMergeStrategy _mergeStrategy;
    private IExecutionStrategy? _executionStrategy;

    /// <summary>
    ///     Initializes a new instance of the PriorityBasedMergeNode class
    /// </summary>
    /// <param name="mergeStrategy">The merge strategy to use</param>
    /// <param name="logger">The logger</param>
    public PriorityBasedMergeNode(
        PriorityMergeStrategy mergeStrategy,
        ILogger<PriorityBasedMergeNode>? logger = null)
    {
        _mergeStrategy = mergeStrategy ?? throw new ArgumentNullException(nameof(mergeStrategy));
        _logger = logger;
        Metrics = new MergeNodeMetrics();
    }

    public MergeNodeMetrics Metrics { get; }

    // ITransformNode required properties
    public IExecutionStrategy ExecutionStrategy
    {
        get => _executionStrategy ??= new SequentialExecutionStrategy();
        set => _executionStrategy = value;
    }

    public INodeErrorHandler? ErrorHandler { get; set; }

    // ITransformNode implementation (required for pipeline builder compatibility)
    public async Task<MarketDataTick> ExecuteAsync(MarketDataTick input, PipelineContext context, CancellationToken cancellationToken)
    {
        // For transform node compatibility, just pass through the input
        // The actual merge logic is handled by MergeAsync
        return await Task.FromResult(input);
    }

    // CustomMergeNode implementation
    /// <summary>
    ///     Override the MergeAsync method to implement custom merging logic for market data streams.
    ///     This method coordinates multiple input pipes and applies the configured merge strategy.
    /// </summary>
    /// <param name="pipes">Collection of input data pipes from various exchanges</param>
    /// <param name="cancellationToken">Token for cancellation support</param>
    /// <returns>Output data pipe containing merged market data</returns>
    public override Task<IDataPipe<MarketDataTick>> MergeAsync(IEnumerable<IDataPipe> pipes, CancellationToken cancellationToken = default)
    {
        // Convert to typed pipes once so we can inspect and reuse the collection without re-enumerating the enumerable provided by the pipeline runtime
        var typedPipes = pipes.Cast<IDataPipe<MarketDataTick>>().ToList();

        if (typedPipes.Count == 0)
        {
            _logger?.LogWarning("No input pipes provided to merge node");

            // Return empty pipe for graceful handling
            return Task.FromResult<IDataPipe<MarketDataTick>>(new ChannelDataPipe<MarketDataTick>(Channel.CreateBounded<MarketDataTick>(1), "EmptyMerge"));
        }

        _logger?.LogInformation("Starting merge operation with {Count} input pipes", typedPipes.Count);

        // Refresh metrics for the current run
        Metrics.UpdateActiveSources(typedPipes.Count);

        try
        {
            // Execute the configured merge strategy with all input pipes
            _mergeStrategy.Metrics = Metrics;
            var resultPipe = _mergeStrategy.Merge(typedPipes, cancellationToken);

            _logger?.LogInformation("Merge operation completed successfully");
            return Task.FromResult(resultPipe);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Merge operation failed");
            throw;
        }
    }
}
