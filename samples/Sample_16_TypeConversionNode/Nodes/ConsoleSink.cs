using System;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_16_TypeConversionNode.Nodes;

/// <summary>
///     Generic console sink node that outputs data with formatted display.
///     This node demonstrates outputting converted data to console for debugging and monitoring.
/// </summary>
/// <remarks>
///     This sink showcases output patterns:
///     - Formatted console output with type information
///     - Multiple data type support through generic implementation
///     - Color-coded output based on data characteristics
///     - Summary statistics and progress reporting
///     - Error handling for output operations
///     This pattern is useful for debugging, monitoring, and development scenarios.
/// </remarks>
public sealed class ConsoleSink<T> : SinkNode<T>
{
    private readonly string _prefix;
    private int _count;

    /// <summary>
    ///     Initializes a new instance of <see cref="ConsoleSink{T}" /> class.
    /// </summary>
    /// <param name="prefix">The prefix to display before output.</param>
    public ConsoleSink(string prefix = "Output")
    {
        _prefix = prefix;
        _count = 0;
    }

    /// <summary>
    ///     Outputs items to console with formatting based on data type.
    /// </summary>
    /// <param name="input">The input data to output.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing output operation.</returns>
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            _count++;
            OutputItem(item, _count);
        }

        // Output summary
        Console.WriteLine($"\n{_prefix} Summary: Processed {_count} items");
        Console.WriteLine(new string('=', 50));
    }

    /// <summary>
    ///     Outputs a single item with type-specific formatting.
    /// </summary>
    /// <param name="item">The item to output.</param>
    /// <param name="index">The item index.</param>
    private void OutputItem(T item, int index)
    {
        var itemType = item?.GetType().Name ?? "Unknown";

        Console.WriteLine($"\n{_prefix} #{index} [{itemType}]:");
        Console.WriteLine(new string('-', 40));

        switch (item)
        {
            case SensorReading sensorReading:
                OutputSensorReading(sensorReading);
                break;
            case SensorDto sensorDto:
                OutputSensorDto(sensorDto);
                break;
            case CanonicalSensorData canonicalData:
                OutputCanonicalSensorData(canonicalData);
                break;
            case SensorData sensorData:
                OutputSensorData(sensorData);
                break;
            default:
                Console.WriteLine(item?.ToString() ?? "null");
                break;
        }
    }

    /// <summary>
    ///     Outputs SensorReading with detailed formatting.
    /// </summary>
    /// <param name="reading">The sensor reading to output.</param>
    private static void OutputSensorReading(SensorReading reading)
    {
        var statusColor = reading.IsValid
            ? ConsoleColor.Green
            : ConsoleColor.Red;

        WriteColoredLine($"  ID: {reading.Id}", ConsoleColor.White);
        WriteColoredLine($"  Timestamp: {reading.Timestamp:yyyy-MM-dd HH:mm:ss}", ConsoleColor.White);
        WriteColoredLine($"  Temperature: {reading.Temperature:F2}°C ({reading.TemperatureFahrenheit:F2}°F)", ConsoleColor.Cyan);
        WriteColoredLine($"  Humidity: {reading.Humidity:F1}%", ConsoleColor.Cyan);
        WriteColoredLine($"  Pressure: {reading.Pressure:F1} hPa", ConsoleColor.Cyan);
        WriteColoredLine($"  Sensor Type: {reading.SensorType}", ConsoleColor.White);
        WriteColoredLine($"  Status: {reading.Status}", ConsoleColor.White);
        WriteColoredLine($"  Location: {reading.Location}", ConsoleColor.White);
        WriteColoredLine($"  Processed At: {reading.ProcessedAt:yyyy-MM-dd HH:mm:ss}", ConsoleColor.Gray);
        WriteColoredLine($"  Valid: {reading.IsValid}", statusColor);

        if (!reading.IsValid)
            WriteColoredLine($"  Validation: {reading.ValidationMessage}", ConsoleColor.Yellow);
    }

    /// <summary>
    ///     Outputs SensorDto with API-style formatting.
    /// </summary>
    /// <param name="dto">The sensor DTO to output.</param>
    private static void OutputSensorDto(SensorDto dto)
    {
        WriteColoredLine($"  sensor_id: {dto.sensor_id}", ConsoleColor.White);
        WriteColoredLine($"  timestamp: {dto.timestamp}", ConsoleColor.White);
        WriteColoredLine($"  temperature_celsius: {dto.temperature_celsius}", ConsoleColor.Cyan);
        WriteColoredLine($"  temperature_fahrenheit: {dto.temperature_fahrenheit}", ConsoleColor.Cyan);
        WriteColoredLine($"  humidity_percent: {dto.humidity_percent}", ConsoleColor.Cyan);
        WriteColoredLine($"  pressure_hpa: {dto.pressure_hpa}", ConsoleColor.Cyan);
        WriteColoredLine($"  sensor_type: {dto.sensor_type}", ConsoleColor.White);
        WriteColoredLine($"  status: {dto.status}", ConsoleColor.White);
        WriteColoredLine($"  location: {dto.location}", ConsoleColor.White);

        WriteColoredLine($"  is_valid: {dto.is_valid}", dto.is_valid
            ? ConsoleColor.Green
            : ConsoleColor.Red);
    }

    /// <summary>
    ///     Outputs CanonicalSensorData with enterprise formatting.
    /// </summary>
    /// <param name="data">The canonical sensor data to output.</param>
    private static void OutputCanonicalSensorData(CanonicalSensorData data)
    {
        WriteColoredLine($"  SensorId: {data.SensorId}", ConsoleColor.White);
        WriteColoredLine($"  ReadingTimestamp: {data.ReadingTimestamp:yyyy-MM-dd HH:mm:ss zzz}", ConsoleColor.White);
        WriteColoredLine($"  Temperature: {data.TemperatureCelsius:F2}°C / {data.TemperatureFahrenheit:F2}°F", ConsoleColor.Cyan);
        WriteColoredLine($"  Humidity: {data.RelativeHumidity:F1}%", ConsoleColor.Cyan);
        WriteColoredLine($"  Pressure: {data.AtmosphericPressure:F1} hPa", ConsoleColor.Cyan);
        WriteColoredLine($"  Category: {data.Category}", ConsoleColor.White);
        WriteColoredLine($"  State: {data.State}", ConsoleColor.White);
        WriteColoredLine($"  Quality: {data.Quality}", GetQualityColor(data.Quality));
        WriteColoredLine($"  Location: ({data.Location.Latitude:F4}, {data.Location.Longitude:F4})", ConsoleColor.White);

        if (data.Location.Altitude.HasValue)
            WriteColoredLine($"  Altitude: {data.Location.Altitude:F1}m", ConsoleColor.White);

        if (!string.IsNullOrEmpty(data.Location.Name))
            WriteColoredLine($"  Location Name: {data.Location.Name}", ConsoleColor.White);

        if (data.Metadata.Count > 0)
        {
            WriteColoredLine("  Metadata:", ConsoleColor.Gray);

            foreach (var kvp in data.Metadata)
            {
                WriteColoredLine($"    {kvp.Key}: {kvp.Value}", ConsoleColor.Gray);
            }
        }
    }

    /// <summary>
    ///     Outputs SensorData with basic formatting.
    /// </summary>
    /// <param name="data">The sensor data to output.</param>
    private static void OutputSensorData(SensorData data)
    {
        WriteColoredLine($"  ID: {data.Id}", ConsoleColor.White);
        WriteColoredLine($"  Timestamp: {data.Timestamp:yyyy-MM-dd HH:mm:ss}", ConsoleColor.White);
        WriteColoredLine($"  Temperature: {data.Temperature:F2}°C", ConsoleColor.Cyan);
        WriteColoredLine($"  Humidity: {data.Humidity:F1}%", ConsoleColor.Cyan);
        WriteColoredLine($"  Pressure: {data.Pressure:F1} hPa", ConsoleColor.Cyan);
        WriteColoredLine($"  Sensor Type: {data.SensorType}", ConsoleColor.White);
        WriteColoredLine($"  Status: {data.Status}", ConsoleColor.White);
    }

    /// <summary>
    ///     Gets console color based on data quality.
    /// </summary>
    /// <param name="quality">The data quality enum.</param>
    /// <returns>Console color representing quality level.</returns>
    private static ConsoleColor GetQualityColor(DataQuality quality)
    {
        return quality switch
        {
            DataQuality.High => ConsoleColor.Green,
            DataQuality.Medium => ConsoleColor.Yellow,
            DataQuality.Low => ConsoleColor.Red,
            DataQuality.Unknown => ConsoleColor.Gray,
            _ => ConsoleColor.White,
        };
    }

    /// <summary>
    ///     Writes a line to console with specified color.
    /// </summary>
    /// <param name="text">The text to write.</param>
    /// <param name="color">The color to use.</param>
    private static void WriteColoredLine(string text, ConsoleColor color)
    {
        var originalColor = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.WriteLine(text);
        Console.ForegroundColor = originalColor;
    }
}
