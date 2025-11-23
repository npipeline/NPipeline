using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Execution;
using Sample_CustomMergeNode.Infrastructure;
using Sample_CustomMergeNode.Models;

namespace Sample_CustomMergeNode.Strategies;

/// <summary>
///     Priority-based merge strategy for market data
/// </summary>
public class PriorityMergeStrategy : IMergeStrategy<MarketDataTick>
{
    private readonly int _bufferSize;
    private readonly TimeSpan _delayTolerance;
    private readonly ILogger<PriorityMergeStrategy>? _logger;
    private readonly TemporalAlignmentStrategy _temporalAlignment;

    /// <summary>
    ///     Initializes a new instance of the PriorityMergeStrategy class
    /// </summary>
    /// <param name="logger">The logger</param>
    /// <param name="delayTolerance">Maximum delay tolerance for temporal alignment</param>
    /// <param name="bufferSize">Buffer size for temporal alignment</param>
    public PriorityMergeStrategy(
        ILogger<PriorityMergeStrategy>? logger = null,
        TimeSpan? delayTolerance = null,
        int bufferSize = 1000)
    {
        _logger = logger;
        _delayTolerance = delayTolerance ?? TimeSpan.FromMilliseconds(50);
        _bufferSize = bufferSize;
        _temporalAlignment = new TemporalAlignmentStrategy(_delayTolerance, null);
    }

    /// <summary>
    ///     Gets or sets the metrics sink used to observe merge execution.
    /// </summary>
    public MergeNodeMetrics? Metrics { get; set; }

    /// <summary>
    ///     Implements priority-based merging for market data from multiple exchanges.
    ///     Higher priority exchanges (NYSE > NASDAQ > International) take precedence.
    /// </summary>
    /// <param name="pipes">Collection of input data pipes from various exchanges</param>
    /// <param name="cancellationToken">Token for cancellation support</param>
    /// <returns>Output data pipe containing merged market data</returns>
    public IDataPipe<MarketDataTick> Merge(IEnumerable<IDataPipe<MarketDataTick>> pipes, CancellationToken cancellationToken = default)
    {
        var pipeList = pipes.ToList();

        if (pipeList.Count == 0)
        {
            _logger?.LogWarning("Priority merge invoked with no input pipes");
            return new ChannelDataPipe<MarketDataTick>(Channel.CreateBounded<MarketDataTick>(1), "PriorityMergeEmpty");
        }

        Metrics?.UpdateActiveSources(pipeList.Count);
        Metrics?.UpdateBufferSize(_bufferSize);

        _logger?.LogInformation("Starting priority-based merge with {Count} input pipes", pipeList.Count);

        var inboundChannel = Channel.CreateUnbounded<MarketDataTick>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false,
        });

        var outputChannel = Channel.CreateBounded<MarketDataTick>(new BoundedChannelOptions(_bufferSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });

        // Stage 1: fan-in all source pipes into a single inbound channel
        _ = Task.Run(async () =>
        {
            try
            {
                var forwarders = pipeList
                    .Select(pipe => ForwardPipeAsync(pipe, inboundChannel.Writer, cancellationToken))
                    .ToArray();

                await Task.WhenAll(forwarders).ConfigureAwait(false);
                _ = inboundChannel.Writer.TryComplete();
            }
            catch (OperationCanceledException)
            {
                _ = inboundChannel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error forwarding input pipes during merge");
                _ = inboundChannel.Writer.TryComplete(ex);
            }
        }, CancellationToken.None);

        // Stage 2: consume inbound ticks and apply merge policy before emitting to the output channel
        _ = Task.Run(async () =>
        {
            try
            {
                await MergeLoopAsync(inboundChannel.Reader, outputChannel.Writer, cancellationToken).ConfigureAwait(false);
                _ = outputChannel.Writer.TryComplete();
            }
            catch (OperationCanceledException)
            {
                _ = outputChannel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Priority merge loop encountered an error");
                _ = outputChannel.Writer.TryComplete(ex);
            }
        }, CancellationToken.None);

        return new ChannelDataPipe<MarketDataTick>(outputChannel, "PriorityMergeOutput");
    }

    private async Task ForwardPipeAsync(
        IDataPipe<MarketDataTick> pipe,
        ChannelWriter<MarketDataTick> inboundWriter,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var tick in pipe.WithCancellation(cancellationToken))
            {
                Metrics?.IncrementTotalTicksProcessed();
                await inboundWriter.WriteAsync(tick, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Respect cancellation without logging noise.
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error reading from pipe {PipeName}", pipe.StreamName);
            throw;
        }
    }

    private async Task MergeLoopAsync(
        ChannelReader<MarketDataTick> inboundReader,
        ChannelWriter<MarketDataTick> outputWriter,
        CancellationToken cancellationToken)
    {
        var latestBySymbol = new Dictionary<string, MarketDataTick>(StringComparer.OrdinalIgnoreCase);

        await foreach (var incomingTick in inboundReader.ReadAllAsync(cancellationToken))
        {
            Metrics?.IncrementMergeOperations();

            var alignedTick = _temporalAlignment.ProcessTick(incomingTick);

            if (alignedTick is null)
            {
                Metrics?.IncrementFailedMerges();
                continue;
            }

            if (!latestBySymbol.TryGetValue(alignedTick.Symbol, out var currentBest))
            {
                latestBySymbol[alignedTick.Symbol] = alignedTick;
                await EmitAsync(alignedTick, outputWriter, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (ShouldEmit(alignedTick, currentBest))
            {
                alignedTick.PreviousTick = currentBest;
                latestBySymbol[alignedTick.Symbol] = alignedTick;

                if ((alignedTick.QualityScore?.OverallScore ?? 0) > (currentBest.QualityScore?.OverallScore ?? 0))
                    Metrics?.IncrementQualityImprovements();

                if (!string.Equals(alignedTick.Exchange, currentBest.Exchange, StringComparison.OrdinalIgnoreCase))
                    Metrics?.IncrementConflictsResolved();

                await EmitAsync(alignedTick, outputWriter, cancellationToken).ConfigureAwait(false);
            }
            else
                Metrics?.IncrementFailedMerges();
        }
    }

    private async Task EmitAsync(
        MarketDataTick tick,
        ChannelWriter<MarketDataTick> writer,
        CancellationToken cancellationToken)
    {
        Metrics?.IncrementSuccessfulMerges();
        await writer.WriteAsync(tick, cancellationToken).ConfigureAwait(false);
    }

    private static bool ShouldEmit(MarketDataTick candidate, MarketDataTick current)
    {
        if (candidate.Priority != current.Priority)
            return candidate.Priority > current.Priority;

        if (candidate.Timestamp > current.Timestamp)
            return true;

        if (candidate.Timestamp < current.Timestamp)
            return false;

        var candidateQuality = candidate.QualityScore?.OverallScore ?? 0;
        var currentQuality = current.QualityScore?.OverallScore ?? 0;

        if (candidateQuality > currentQuality + 0.1)
            return true;

        if (candidateQuality + 0.1 < currentQuality)
            return false;

        // Emit if core market data differs meaningfully (price/volume changes)
        return candidate.Price != current.Price || candidate.Volume != current.Volume;
    }
}
