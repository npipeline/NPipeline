using Sample_LookupNode.Models;

namespace Sample_LookupNode.Helpers;

/// <summary>
///     In-memory device registry that provides sample IoT device metadata.
///     This class simulates a device database for the LookupNode sample.
/// </summary>
public static class DeviceRegistry
{
    private static readonly Dictionary<string, DeviceMetadata> _devices = new();

    static DeviceRegistry()
    {
        InitializeSampleDevices();
    }

    /// <summary>
    ///     Gets device metadata by device ID.
    /// </summary>
    /// <param name="deviceId">The unique identifier of the device.</param>
    /// <returns>Device metadata if found, null otherwise.</returns>
    public static DeviceMetadata? GetDevice(string deviceId)
    {
        _devices.TryGetValue(deviceId, out var device);
        return device;
    }

    /// <summary>
    ///     Gets all registered devices.
    /// </summary>
    /// <returns>All devices in the registry.</returns>
    public static IEnumerable<DeviceMetadata> GetAllDevices()
    {
        return _devices.Values;
    }

    /// <summary>
    ///     Initializes the registry with sample IoT devices.
    /// </summary>
    private static void InitializeSampleDevices()
    {
        var now = DateTime.UtcNow;

        // Temperature sensors
        AddDevice("TEMP-001", "Factory A - Production Line 1", "Temperature Sensor",
            now.AddYears(-2), now.AddDays(-30), now.AddDays(335), "Online",
            new Dictionary<string, object> { ["Range"] = "-40°C to 125°C", ["Accuracy"] = "±0.5°C", ["Model"] = "TempSense Pro" });

        AddDevice("TEMP-002", "Factory A - Production Line 2", "Temperature Sensor",
            now.AddYears(-1), now.AddDays(-15), now.AddDays(350), "Online",
            new Dictionary<string, object> { ["Range"] = "-40°C to 125°C", ["Accuracy"] = "±0.3°C", ["Model"] = "TempSense Elite" });

        AddDevice("TEMP-003", "Factory B - Storage Area", "Temperature Sensor",
            now.AddYears(-3), now.AddDays(-90), now.AddDays(-5), "CalibrationRequired",
            new Dictionary<string, object> { ["Range"] = "-20°C to 80°C", ["Accuracy"] = "±1.0°C", ["Model"] = "TempSense Basic" });

        // Pressure sensors
        AddDevice("PRES-001", "Factory A - Boiler Room", "Pressure Sensor",
            now.AddMonths(-18), now.AddDays(-45), now.AddDays(320), "Online",
            new Dictionary<string, object> { ["Range"] = "0-300 PSI", ["Accuracy"] = "±1% FS", ["Model"] = "PressGuard 3000" });

        AddDevice("PRES-002", "Factory B - Compressor Room", "Pressure Sensor",
            now.AddMonths(-24), now.AddDays(-120), now.AddDays(245), "Online",
            new Dictionary<string, object> { ["Range"] = "0-500 PSI", ["Accuracy"] = "±0.5% FS", ["Model"] = "PressGuard 5000" });

        AddDevice("PRES-003", "Factory A - Hydraulic System", "Pressure Sensor",
            now.AddYears(-2), now.AddDays(-200), now.AddDays(165), "Maintenance",
            new Dictionary<string, object> { ["Range"] = "0-1000 PSI", ["Accuracy"] = "±0.25% FS", ["Model"] = "PressGuard Pro" });

        // Humidity sensors
        AddDevice("HUM-001", "Factory A - Clean Room", "Humidity Sensor",
            now.AddMonths(-12), now.AddDays(-20), now.AddDays(345), "Online",
            new Dictionary<string, object> { ["Range"] = "0-100% RH", ["Accuracy"] = "±2% RH", ["Model"] = "HumidSense Pro" });

        AddDevice("HUM-002", "Factory B - Warehouse", "Humidity Sensor",
            now.AddMonths(-8), now.AddDays(-10), now.AddDays(355), "Online",
            new Dictionary<string, object> { ["Range"] = "0-100% RH", ["Accuracy"] = "±3% RH", ["Model"] = "HumidSense Basic" });

        AddDevice("HUM-003", "Factory A - Server Room", "Humidity Sensor",
            now.AddYears(-1), now.AddDays(-400), now.AddDays(-35), "Offline",
            new Dictionary<string, object> { ["Range"] = "0-100% RH", ["Accuracy"] = "±1% RH", ["Model"] = "HumidSense Elite" });

        // Vibration sensors
        AddDevice("VIB-001", "Factory A - Motor 1", "Vibration Sensor",
            now.AddMonths(-6), now.AddDays(-5), now.AddDays(360), "Online",
            new Dictionary<string, object> { ["Range"] = "0-100 mm/s", ["Accuracy"] = "±5%", ["Model"] = "VibDetect Pro" });

        AddDevice("VIB-002", "Factory B - Motor 2", "Vibration Sensor",
            now.AddMonths(-15), now.AddDays(-60), now.AddDays(305), "Online",
            new Dictionary<string, object> { ["Range"] = "0-200 mm/s", ["Accuracy"] = "±3%", ["Model"] = "VibDetect Elite" });

        AddDevice("VIB-003", "Factory A - Pump Station", "Vibration Sensor",
            now.AddMonths(-9), now.AddDays(-150), now.AddDays(215), "Error",
            new Dictionary<string, object> { ["Range"] = "0-50 mm/s", ["Accuracy"] = "±10%", ["Model"] = "VibDetect Basic" });
    }

    /// <summary>
    ///     Adds a device to the registry.
    /// </summary>
    private static void AddDevice(
        string deviceId,
        string location,
        string deviceType,
        DateTime installationDate,
        DateTime lastCalibration,
        DateTime nextCalibrationDue,
        string status,
        Dictionary<string, object> properties)
    {
        var device = new DeviceMetadata(
            deviceId,
            location,
            deviceType,
            installationDate,
            lastCalibration,
            nextCalibrationDue,
            status,
            properties
        );

        _devices[deviceId] = device;
    }
}
