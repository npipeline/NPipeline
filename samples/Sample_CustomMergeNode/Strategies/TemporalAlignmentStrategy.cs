using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Sample_CustomMergeNode.Models;

namespace Sample_CustomMergeNode.Strategies;

/// <summary>
///     Implements temporal alignment strategy for market data
/// </summary>
public class TemporalAlignmentStrategy
{
    private readonly int _delayToleranceMs;
    private readonly ConcurrentDictionary<string, DateTime> _lastProcessedTimes = new();
    private readonly object _lockObject = new();
    private readonly ILogger<TemporalAlignmentStrategy>? _logger;
    private readonly ConcurrentDictionary<string, Queue<MarketDataTick>> _tickQueues = new();

    /// <summary>
    ///     Initializes a new instance of the TemporalAlignmentStrategy class
    /// </summary>
    /// <param name="delayTolerance">The delay tolerance</param>
    /// <param name="logger">The logger</param>
    public TemporalAlignmentStrategy(
        TimeSpan delayTolerance,
        ILogger<TemporalAlignmentStrategy>? logger = null)
    {
        _delayToleranceMs = (int)delayTolerance.TotalMilliseconds;
        _logger = logger;
    }

    /// <summary>
    ///     Aligns a single tick temporally
    /// </summary>
    /// <param name="tick">The tick to align</param>
    /// <param name="buffer">The buffer for temporal alignment</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The aligned tick or null if it should be dropped</returns>
    public Task<MarketDataTick?> AlignTickAsync(
        MarketDataTick tick,
        ConcurrentDictionary<string, List<MarketDataTick>> buffer,
        CancellationToken cancellationToken = default)
    {
        if (tick == null)
            return Task.FromResult<MarketDataTick?>(null);

        var delay = (DateTime.UtcNow - tick.Timestamp).TotalMilliseconds;

        if (delay > _delayToleranceMs)
        {
            _logger?.LogDebug("Dropped {Symbol} tick from {Exchange} - delay of {DelayMs}ms exceeds tolerance of {ToleranceMs}ms",
                tick.Symbol, tick.Exchange, delay, _delayToleranceMs);

            return Task.FromResult<MarketDataTick?>(null);
        }

        // Add to buffer
        buffer.AddOrUpdate(tick.Symbol,
            new List<MarketDataTick> { tick },
            (_, list) =>
            {
                list.Add(tick);
                return list;
            });

        // Process the buffer for this symbol
        if (buffer.TryGetValue(tick.Symbol, out var tickList))
        {
            // Sort by timestamp
            tickList.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));

            // Return the earliest tick that's within tolerance
            var earliestTick = tickList.FirstOrDefault();

            if (earliestTick != null && IsWithinDelayTolerance(earliestTick))
            {
                tickList.Remove(earliestTick);
                return Task.FromResult(earliestTick)!;
            }
        }

        return Task.FromResult<MarketDataTick?>(null);
    }

    /// <summary>
    ///     Aligns ticks temporally based on delay tolerance
    /// </summary>
    /// <param name="ticks">The ticks to align</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The aligned ticks</returns>
    public Task<List<MarketDataTick>> AlignTicksAsync(IEnumerable<MarketDataTick> ticks, CancellationToken cancellationToken = default)
    {
        if (ticks == null)
        {
            _logger?.LogWarning("Received null ticks collection");
            return Task.FromResult<List<MarketDataTick>>([]);
        }

        var tickList = ticks.ToList();

        if (tickList.Count == 0)
        {
            _logger?.LogDebug("Received empty ticks collection");
            return Task.FromResult(new List<MarketDataTick>());
        }

        _logger?.LogDebug("Aligning {TickCount} ticks temporally with delay tolerance of {DelayToleranceMs}ms", tickList.Count, _delayToleranceMs);

        try
        {
            var alignedTicks = new List<MarketDataTick>();

            foreach (var tick in tickList)
            {
                if (IsWithinDelayTolerance(tick))
                    alignedTicks.Add(tick);
            }

            _logger?.LogDebug("Aligned {AlignedTickCount} ticks out of {InputTickCount} input ticks", alignedTicks.Count, tickList.Count);
            return Task.FromResult(alignedTicks);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error aligning ticks temporally");
            return Task.FromResult(new List<MarketDataTick>());
        }
    }

    /// <summary>
    ///     Checks if a tick is within the delay tolerance
    /// </summary>
    /// <param name="tick">The tick to check</param>
    /// <returns>True if within tolerance, false otherwise</returns>
    private bool IsWithinDelayTolerance(MarketDataTick tick)
    {
        if (tick == null)
            return false;

        var delay = (DateTime.UtcNow - tick.Timestamp).TotalMilliseconds;

        if (delay > _delayToleranceMs)
        {
            _logger?.LogDebug("Dropped {Symbol} tick from {Exchange} - delay of {DelayMs}ms exceeds tolerance of {ToleranceMs}ms",
                tick.Symbol, tick.Exchange, delay, _delayToleranceMs);

            return false;
        }

        return true;
    }

    /// <summary>
    ///     Processes a tick for temporal alignment
    /// </summary>
    /// <param name="tick">The tick to process</param>
    /// <returns>The aligned tick or null if it should be dropped</returns>
    public MarketDataTick? ProcessTick(MarketDataTick tick)
    {
        if (tick == null)
            return null;

        var delay = (DateTime.UtcNow - tick.Timestamp).TotalMilliseconds;

        if (delay > _delayToleranceMs)
        {
            _logger?.LogDebug("Dropped {Symbol} tick from {Exchange} - delay of {DelayMs}ms exceeds tolerance of {ToleranceMs}ms",
                tick.Symbol, tick.Exchange, delay, _delayToleranceMs);

            return null;
        }

        lock (_lockObject)
        {
            var symbol = tick.Symbol;

            if (!_tickQueues.TryGetValue(symbol, out var queue))
            {
                queue = new Queue<MarketDataTick>();
                _tickQueues[symbol] = queue;
            }

            queue.Enqueue(tick);

            // Process the queue
            while (queue.Count > 0)
            {
                var earliestTick = queue.Peek();
                var earliestDelay = (DateTime.UtcNow - earliestTick.Timestamp).TotalMilliseconds;

                // Check if it's within the delay tolerance
                if (earliestDelay <= _delayToleranceMs)
                {
                    queue.Dequeue();

                    // Update the last processed time
                    _lastProcessedTimes.AddOrUpdate(symbol, earliestTick.Timestamp, (_, _) => earliestTick.Timestamp);
                    return earliestTick;
                }

                // Drop the tick if it's too old
                if (earliestDelay > _delayToleranceMs * 2)
                {
                    _logger?.LogDebug("Dropped old {Symbol} tick from {Exchange} - timestamp {Timestamp}",
                        symbol, tick.Exchange, tick.Timestamp);

                    queue.Dequeue();
                }
                else
                {
                    // Wait for more recent ticks
                    break;
                }
            }
        }

        return null;
    }

    /// <summary>
    ///     Cleans up old data to prevent memory leaks
    /// </summary>
    public void Cleanup()
    {
        lock (_lockObject)
        {
            var now = DateTime.UtcNow;
            var keysToRemove = new List<string>();

            foreach (var kvp in _lastProcessedTimes)
            {
                if ((now - kvp.Value).TotalMinutes > 5)
                    keysToRemove.Add(kvp.Key);
            }

            foreach (var key in keysToRemove)
            {
                _tickQueues.TryRemove(key, out _);
                _lastProcessedTimes.TryRemove(key, out _);
            }
        }

        _logger?.LogDebug("Cleaned up old temporal alignment data");
    }
}
