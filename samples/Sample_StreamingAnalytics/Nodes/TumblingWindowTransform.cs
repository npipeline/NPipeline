using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_StreamingAnalytics.Nodes;

/// <summary>
///     Transform node that implements tumbling window processing.
///     Tumbling windows are non-overlapping, fixed-size time windows that process data in discrete chunks.
/// </summary>
public class TumblingWindowTransform : TransformNode<TimeSeriesData, WindowedResult>
{
    private readonly TimeSpan _allowedLateness = TimeSpan.FromSeconds(10); // 10-second allowed lateness
    private readonly List<TimeSeriesData> _currentWindow;
    private readonly TimeSpan _windowSize = TimeSpan.FromSeconds(5); // 5-second windows
    private bool _isFirstWindow;
    private DateTime _lastWindowEnd;
    private DateTime _windowStart;

    /// <summary>
    ///     Initializes a new instance of the TumblingWindowTransform class.
    /// </summary>
    public TumblingWindowTransform()
    {
        _currentWindow = new List<TimeSeriesData>();
        _isFirstWindow = true;
    }

    /// <summary>
    ///     Processes time-series data points and creates tumbling window results.
    /// </summary>
    /// <param name="item">The time-series data point to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A task containing the windowed result when a window is complete.</returns>
    public override async Task<WindowedResult> ExecuteAsync(TimeSeriesData item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Initialize the first window
        if (_isFirstWindow)
        {
            _windowStart = item.Timestamp;
            _lastWindowEnd = _windowStart + _windowSize;
            _isFirstWindow = false;
            Console.WriteLine($"Starting first tumbling window at {_windowStart:O}");
        }

        // Determine the window for this data point
        var dataTimestamp = item.IsLate && item.OriginalTimestamp.HasValue
            ? item.OriginalTimestamp.Value
            : item.Timestamp;

        // Check if we need to advance to a new window
        if (dataTimestamp >= _lastWindowEnd)
        {
            // Emit the current window result if it has data
            WindowedResult result;

            if (_currentWindow.Count > 0)
            {
                result = CreateWindowResult();
                Console.WriteLine($"Emitting tumbling window: {_windowStart:O} - {_lastWindowEnd:O} with {_currentWindow.Count} items");
            }
            else
            {
                // Create empty result when no data
                result = new WindowedResult
                {
                    WindowStart = _windowStart,
                    WindowEnd = _lastWindowEnd,
                    WindowType = "Tumbling",
                    Count = 0,
                    Sum = 0,
                    Average = 0,
                    Min = 0,
                    Max = 0,
                    LateCount = 0,
                    Sources = new HashSet<string>(),
                };
            }

            // Advance to the next window
            AdvanceWindow(dataTimestamp);

            // Clear the current window and add the new item
            _currentWindow.Clear();
            _currentWindow.Add(item);

            return await Task.FromResult(result);
        }

        // Add the item to the current window if it belongs here
        if (dataTimestamp >= _windowStart && dataTimestamp < _lastWindowEnd)
        {
            _currentWindow.Add(item);

            return await Task.FromResult(new WindowedResult
            {
                WindowStart = _windowStart,
                WindowEnd = _lastWindowEnd,
                WindowType = "Tumbling",
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
        if (item.IsLate && dataTimestamp < _windowStart - _allowedLateness)
        {
            Console.WriteLine($"Dropping late data point: {item.Id} (too late for window {_windowStart:O})");

            return await Task.FromResult(new WindowedResult
            {
                WindowStart = _windowStart,
                WindowEnd = _lastWindowEnd,
                WindowType = "Tumbling",
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
            WindowStart = _windowStart,
            WindowEnd = _lastWindowEnd,
            WindowType = "Tumbling",
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
        if (_currentWindow.Count == 0)
            throw new InvalidOperationException("Cannot create window result from empty window");

        var values = _currentWindow.Select(d => d.Value).ToList();
        var sources = new HashSet<string>(_currentWindow.Select(d => d.Source));
        var lateCount = _currentWindow.Count(d => d.IsLate);

        return new WindowedResult
        {
            WindowStart = _windowStart,
            WindowEnd = _lastWindowEnd,
            WindowType = "Tumbling",
            Count = _currentWindow.Count,
            Sum = values.Sum(),
            Average = values.Average(),
            Min = values.Min(),
            Max = values.Max(),
            LateCount = lateCount,
            Sources = sources,
        };
    }

    /// <summary>
    ///     Advances the window to the next time period.
    /// </summary>
    private void AdvanceWindow(DateTime dataTimestamp)
    {
        // Calculate how many windows to advance
        var windowsToAdvance = (int)Math.Ceiling((dataTimestamp - _windowStart).TotalMilliseconds / _windowSize.TotalMilliseconds);

        _windowStart = _windowStart.Add(TimeSpan.FromMilliseconds(windowsToAdvance * _windowSize.TotalMilliseconds));
        _lastWindowEnd = _windowStart + _windowSize;

        Console.WriteLine($"Advanced tumbling window to: {_windowStart:O} - {_lastWindowEnd:O}");
    }

    /// <summary>
    ///     Finalizes processing by emitting any remaining window data.
    /// </summary>
    /// <returns>The final windowed result if there is remaining data.</returns>
    public WindowedResult? FinalizeWindow()
    {
        if (_currentWindow.Count > 0)
        {
            var result = CreateWindowResult();
            Console.WriteLine($"Finalizing tumbling window: {_windowStart:O} - {_lastWindowEnd:O} with {_currentWindow.Count} items");
            return result;
        }

        return null;
    }
}
