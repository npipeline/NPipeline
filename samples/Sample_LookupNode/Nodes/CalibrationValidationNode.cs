using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_LookupNode.Models;

namespace Sample_LookupNode.Nodes;

/// <summary>
///     Transform node that validates sensor calibration status.
///     This node demonstrates how to create a transform that validates calibration
///     by inheriting from TransformNode&lt;SensorReadingWithMetadata, SensorReadingWithCalibration&gt;.
/// </summary>
public class CalibrationValidationNode : TransformNode<SensorReadingWithMetadata, SensorReadingWithCalibration>
{
    /// <summary>
    ///     Validates the calibration status of the sensor reading.
    /// </summary>
    /// <param name="item">The input item to validate.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the validation result.</returns>
    public override async Task<SensorReadingWithCalibration> ExecuteAsync(SensorReadingWithMetadata item, PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Simulate processing delay
        await Task.Delay(Random.Shared.Next(5, 20), cancellationToken);

        var reading = item.OriginalReading;
        var device = item.DeviceInfo;

        // Check if reading timestamp is within valid calibration period
        var calibrationValid = IsCalibrationValid(reading.Timestamp, device.LastCalibration, device.NextCalibrationDue);
        var daysUntilCalibration = CalculateDaysUntilCalibration(device.NextCalibrationDue);
        var calibrationWarning = GenerateCalibrationWarning(calibrationValid, daysUntilCalibration, device.DeviceStatus);

        Console.WriteLine($"Calibration check for {device.DeviceId}: {(calibrationValid ? "✓ Valid" : "✗ Invalid")} - {calibrationWarning}");

        return new SensorReadingWithCalibration(
            reading,
            device,
            calibrationValid,
            calibrationWarning,
            daysUntilCalibration
        );
    }

    /// <summary>
    ///     Determines if the device calibration is valid for the given reading timestamp.
    /// </summary>
    private static bool IsCalibrationValid(DateTime readingTimestamp, DateTime lastCalibration, DateTime nextCalibrationDue)
    {
        // Reading must be after last calibration and before next calibration is due
        return readingTimestamp >= lastCalibration && readingTimestamp <= nextCalibrationDue;
    }

    /// <summary>
    ///     Calculates the number of days until the next calibration is due.
    /// </summary>
    private static int CalculateDaysUntilCalibration(DateTime nextCalibrationDue)
    {
        return (int)(nextCalibrationDue - DateTime.UtcNow).TotalDays;
    }

    /// <summary>
    ///     Generates an appropriate calibration warning message.
    /// </summary>
    private static string GenerateCalibrationWarning(bool calibrationValid, int daysUntilCalibration, string deviceStatus)
    {
        if (!calibrationValid)
        {
            if (daysUntilCalibration < 0)
                return $"CALIBRATION OVERDUE by {Math.Abs(daysUntilCalibration)} days";

            return "Reading taken outside calibration period";
        }

        if (daysUntilCalibration <= 0)
            return "CALIBRATION DUE TODAY";

        if (daysUntilCalibration <= 7)
            return $"Calibration due soon ({daysUntilCalibration} days)";

        if (daysUntilCalibration <= 30)
            return $"Calibration due in {daysUntilCalibration} days";

        if (deviceStatus.Equals("CalibrationRequired", StringComparison.OrdinalIgnoreCase))
            return "Device status indicates calibration required";

        return "Calibration status OK";
    }
}
