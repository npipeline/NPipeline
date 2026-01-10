using System.Collections.Concurrent;
using NPipeline.Execution;
using NPipeline.Observability;

namespace NPipeline.Extensions.Observability
{
    /// <summary>
    ///     Execution observer that collects metrics during pipeline execution.
    /// </summary>
    public sealed class MetricsCollectingExecutionObserver(IObservabilityCollector collector, bool collectMemoryMetrics = false) : IExecutionObserver, IDisposable
    {
        private readonly bool _collectMemoryMetrics = collectMemoryMetrics;
        private readonly IObservabilityCollector _collector = collector ?? throw new ArgumentNullException(nameof(collector));
        private readonly ConcurrentDictionary<string, long> _nodeInitialMemory = new();
        private readonly ConcurrentDictionary<string, DateTimeOffset> _nodeStartTimes = new();
        private bool _disposed;

        /// <summary>
        ///     Called when a node starts execution.
        /// </summary>
        /// <param name="e">The event containing node execution start information.</param>
        public void OnNodeStarted(NodeExecutionStarted e)
        {
            ArgumentNullException.ThrowIfNull(e);

            if (_disposed)
            {
                return;
            }

            var threadId = Environment.CurrentManagedThreadId;
            long? initialMemoryMb = null;

            if (_collectMemoryMetrics)
            {
                var initialMemoryBytes = GC.GetTotalMemory(false);
                initialMemoryMb = initialMemoryBytes / (1024 * 1024);
                _nodeInitialMemory[e.NodeId] = initialMemoryBytes;
            }

            _collector.RecordNodeStart(e.NodeId, e.StartTime, threadId, initialMemoryMb);
            _nodeStartTimes[e.NodeId] = e.StartTime;
        }

        /// <summary>
        ///     Called when a node completes execution (successfully or with failure).
        /// </summary>
        /// <param name="e">The event containing node execution completion information.</param>
        public void OnNodeCompleted(NodeExecutionCompleted e)
        {
            ArgumentNullException.ThrowIfNull(e);

            if (_disposed)
            {
                return;
            }

            // Only record completion if node was started
            if (!_nodeStartTimes.TryRemove(e.NodeId, out var startTime))
            {
                return;
            }

            var endTime = startTime + e.Duration;

            long? memoryDeltaMb = null;

            if (_collectMemoryMetrics)
            {
                var finalMemoryBytes = GC.GetTotalMemory(false);

                // Calculate per-node delta by subtracting initial memory from final memory
                if (_nodeInitialMemory.TryRemove(e.NodeId, out var initialMemoryBytes))
                {
                    var deltaBytes = finalMemoryBytes - initialMemoryBytes;
                    memoryDeltaMb = deltaBytes / (1024 * 1024);
                }
            }

            long? processorTimeMs = null; // CPU time is not available per-node

            _collector.RecordNodeEnd(
                e.NodeId,
                endTime,
                e.Success,
                e.Error,
                memoryDeltaMb,
                processorTimeMs);

            // Calculate and record performance metrics if items were processed
            var nodeMetrics = _collector.GetNodeMetrics(e.NodeId);

            if (nodeMetrics != null && nodeMetrics.ItemsProcessed > 0 && nodeMetrics.DurationMs.HasValue)
            {
                var durationSec = nodeMetrics.DurationMs.Value / 1000.0;

                if (durationSec > 0)
                {
                    var throughput = nodeMetrics.ItemsProcessed / durationSec;
                    var averageItemProcessingMs = nodeMetrics.DurationMs.Value / (double)nodeMetrics.ItemsProcessed;
                    _collector.RecordPerformanceMetrics(e.NodeId, throughput, averageItemProcessingMs);
                }
            }
        }

        /// <summary>
        ///     Called when a retry operation occurs.
        /// </summary>
        /// <param name="e">The event containing retry information.</param>
        public void OnRetry(NodeRetryEvent e)
        {
            ArgumentNullException.ThrowIfNull(e);

            if (_disposed)
            {
                return;
            }

            var reason = e.LastException?.Message;
            _collector.RecordRetry(e.NodeId, e.Attempt, reason);
        }

        /// <summary>
        ///     Called when items are dropped from a queue due to backpressure.
        /// </summary>
        /// <param name="e">The event containing queue drop information.</param>
        public void OnDrop(QueueDropEvent e)
        {
            ArgumentNullException.ThrowIfNull(e);

            // Queue drops are not directly tracked in node metrics
            // This could be extended to track backpressure metrics
        }

        /// <summary>
        ///     Called with queue metrics information.
        /// </summary>
        /// <param name="e">The event containing queue metrics.</param>
        public void OnQueueMetrics(QueueMetricsEvent e)
        {
            ArgumentNullException.ThrowIfNull(e);

            // Queue metrics are not directly tracked in node metrics
            // This could be extended to track queue depth metrics
        }

        /// <summary>
        ///     Disposes the observer and releases all resources, clearing any accumulated dictionary entries.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///     Disposes the observer and releases all resources.
        /// </summary>
        /// <param name="disposing">True if disposing managed resources; false if called from finalizer.</param>
        private void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                // Clear dictionaries to prevent memory leaks if nodes never completed
                _nodeInitialMemory.Clear();
                _nodeStartTimes.Clear();
            }

            _disposed = true;
        }
    }
}
