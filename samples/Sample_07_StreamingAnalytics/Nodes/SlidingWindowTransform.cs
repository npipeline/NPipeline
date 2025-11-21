using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_07_StreamingAnalytics.Nodes;

/// <summary>
///     Transform node that implements sliding window processing.
///     Sliding windows are overlapping time windows that slide forward by a specified interval.
/// </summary>
public class SlidingWindowTransform : TransformNode<TimeSeriesData, WindowedResult>
{
    private readonly TimeSpan _allowedLateness = TimeSpan.FromSeconds(10); // 10-second allowed lateness
    private readonly TimeSpan _slideInterval = TimeSpan.FromSeconds(2); // Slide every 2 seconds
    private readonly List<TimeSeriesData> _windowData;
    private readonly TimeSpan _windowSize = TimeSpan.FromSeconds(5); // 5-second windows
    private DateTime _currentWindowStart;
    private bool _isInitialized;
    private DateTime _nextSlideTime;

    /// <summary>
    ///     Initializes a new instance of the SlidingWindowTransform class.
    /// </summary>
    public SlidingWindowTransform()
    {
        _windowData = new List<TimeSeriesData>();
        _isInitialized = false;
    }

    /// <summary>
    ///     Processes time-series data points and creates sliding window results.
    /// </summary>
    /// <param name="item">The time-series data point to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A task containing the windowed result when a window slides.</returns>
    public override async Task<WindowedResult> ExecuteAsync(TimeSeriesData item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Initialize the window on the first data point
        if (!_isInitialized)
        {
            var dataTimestamp = item.IsLate && item.OriginalTimestamp.HasValue
                ? item.OriginalTimestamp.Value
                : item.Timestamp;

            _currentWindowStart = dataTimestamp;
            _nextSlideTime = _currentWindowStart + _slideInterval;
            _isInitialized = true;
            Console.WriteLine($"Starting sliding window at {_currentWindowStart:O} (size: {_windowSize}, slide: {_slideInterval})");
        }

        var itemTimestamp = item.IsLate && item.OriginalTimestamp.HasValue
            ? item.OriginalTimestamp.Value
            : item.Timestamp;

        // Check if we need to slide the window
        if (itemTimestamp >= _nextSlideTime)
        {
            // Emit the current window result if it has data
            WindowedResult result;

            if (_windowData.Count > 0)
            {
                result = CreateWindowResult();
                Console.WriteLine($"Emitting sliding window: {_currentWindowStart:O} - {_currentWindowStart + _windowSize:O} with {_windowData.Count} items");
            }
            else
            {
                // Create empty result when no data
                result = new WindowedResult
                {
                    WindowStart = _currentWindowStart,
                    WindowEnd = _currentWindowStart + _windowSize,
                    WindowType = "Sliding",
                    Count = 0,
                    Sum = 0,
                    Average = 0,
                    Min = 0,
                    Max = 0,
                    LateCount = 0,
                    Sources = new HashSet<string>(),
                };
            }

            // Slide the window forward
            while (itemTimestamp >= _nextSlideTime)
            {
                _currentWindowStart = _nextSlideTime;
                _nextSlideTime = _currentWindowStart + _slideInterval;
            }

            // Remove data that's no longer in the window
            var windowEnd = _currentWindowStart + _windowSize;

            _windowData.RemoveAll(d =>
            {
                var dataTimestamp = d.IsLate && d.OriginalTimestamp.HasValue
                    ? d.OriginalTimestamp.Value
                    : d.Timestamp;

                return dataTimestamp < _currentWindowStart;
            });

            Console.WriteLine($"Slid window to: {_currentWindowStart:O} - {windowEnd:O}");

            // Add the new item
            if (itemTimestamp < windowEnd)
                _windowData.Add(item);

            return await Task.FromResult(result);
        }

        // Add the item if it's within the current window
        var currentWindowEnd = _currentWindowStart + _windowSize;

        if (itemTimestamp >= _currentWindowStart && itemTimestamp < currentWindowEnd)
        {
            _windowData.Add(item);

            return await Task.FromResult(new WindowedResult
            {
                WindowStart = _currentWindowStart,
                WindowEnd = currentWindowEnd,
                WindowType = "Sliding",
                Count = 0,
                Sum = 0,
                Average = 0,
                Min = 0,
                Max = 0,
                LateCount = 0,
                Sources = new HashSet<string>(),
            });
        }

        // Handle late-arriving data that falls outside the allowed lateness period
        if (item.IsLate && itemTimestamp < _currentWindowStart - _allowedLateness)
        {
            Console.WriteLine($"Dropping late data point: {item.Id} (too late for sliding window {_currentWindowStart:O})");

            return await Task.FromResult(new WindowedResult
            {
                WindowStart = _currentWindowStart,
                WindowEnd = currentWindowEnd,
                WindowType = "Sliding",
                Count = 0,
                Sum = 0,
                Average = 0,
                Min = 0,
                Max = 0,
                LateCount = 0,
                Sources = new HashSet<string>(),
            });
        }

        return await Task.FromResult(new WindowedResult
        {
            WindowStart = _currentWindowStart,
            WindowEnd = currentWindowEnd,
            WindowType = "Sliding",
            Count = 0,
            Sum = 0,
            Average = 0,
            Min = 0,
            Max = 0,
            LateCount = 0,
            Sources = new HashSet<string>(),
        });
    }

    /// <summary>
    ///     Creates a windowed result from the current window data.
    /// </summary>
    private WindowedResult CreateWindowResult()
    {
        if (_windowData.Count == 0)
            throw new InvalidOperationException("Cannot create window result from empty window");

        var values = _windowData.Select(d => d.Value).ToList();
        var sources = new HashSet<string>(_windowData.Select(d => d.Source));
        var lateCount = _windowData.Count(d => d.IsLate);

        return new WindowedResult
        {
            WindowStart = _currentWindowStart,
            WindowEnd = _currentWindowStart + _windowSize,
            WindowType = "Sliding",
            Count = _windowData.Count,
            Sum = values.Sum(),
            Average = values.Average(),
            Min = values.Min(),
            Max = values.Max(),
            LateCount = lateCount,
            Sources = sources,
        };
    }

    /// <summary>
    ///     Finalizes processing by emitting any remaining window data.
    /// </summary>
    /// <returns>The final windowed result if there is remaining data.</returns>
    public WindowedResult? FinalizeWindow()
    {
        if (_windowData.Count > 0)
        {
            var result = CreateWindowResult();
            Console.WriteLine($"Finalizing sliding window: {_currentWindowStart:O} - {_currentWindowStart + _windowSize:O} with {_windowData.Count} items");
            return result;
        }

        return null;
    }
}
