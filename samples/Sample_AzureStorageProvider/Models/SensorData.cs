namespace Sample_AzureStorageProvider.Models;

/// <summary>
///     Sample data model representing sensor readings for demonstration purposes.
///     Used in CSV processing scenarios to show how to work with structured data.
/// </summary>
public class SensorData
{
    /// <summary>
    ///     Gets or sets the unique identifier for the sensor.
    /// </summary>
    public string SensorId { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the timestamp when the reading was taken.
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    ///     Gets or sets the temperature reading in Celsius.
    /// </summary>
    public double Temperature { get; set; }

    /// <summary>
    ///     Gets or sets the humidity reading as a percentage (0-100).
    /// </summary>
    public double Humidity { get; set; }

    /// <summary>
    ///     Gets or sets the location where the sensor is deployed.
    /// </summary>
    public string Location { get; set; } = string.Empty;

    /// <summary>
    ///     Converts the sensor data to a CSV string representation.
    /// </summary>
    /// <returns>A CSV string containing the sensor data.</returns>
    public string ToCsv()
    {
        return $"{SensorId},{Timestamp:yyyy-MM-dd HH:mm:ss},{Temperature:F2},{Humidity:F2},{Location}";
    }

    /// <summary>
    ///     Parses a CSV string into a SensorData object.
    /// </summary>
    /// <param name="csvLine">The CSV string to parse.</param>
    /// <returns>A SensorData object parsed from the CSV string.</returns>
    /// <exception cref="FormatException">Thrown when the CSV line is malformed.</exception>
    public static SensorData FromCsv(string csvLine)
    {
        var parts = csvLine.Split(',');
        if (parts.Length != 5)
        {
            throw new FormatException($"CSV line must have 5 fields: {csvLine}");
        }

        return new SensorData
        {
            SensorId = parts[0].Trim(),
            Timestamp = DateTime.ParseExact(parts[1].Trim(), "yyyy-MM-dd HH:mm:ss", null),
            Temperature = double.Parse(parts[2].Trim()),
            Humidity = double.Parse(parts[3].Trim()),
            Location = parts[4].Trim()
        };
    }

    /// <summary>
    ///     Returns a string representation of the sensor data.
    /// </summary>
    /// <returns>A formatted string describing the sensor reading.</returns>
    public override string ToString()
    {
        return $"Sensor {SensorId} at {Location}: {Temperature:F1}°C, {Humidity:F1}% humidity at {Timestamp:yyyy-MM-dd HH:mm:ss}";
    }
}
