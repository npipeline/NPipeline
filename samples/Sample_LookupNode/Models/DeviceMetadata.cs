namespace Sample_LookupNode.Models;

/// <summary>
///     Represents metadata for an IoT device used for enrichment.
///     This record contains device configuration and status information.
/// </summary>
/// <param name="DeviceId">Unique identifier for the IoT device.</param>
/// <param name="FactoryLocation">Physical location where the device is installed.</param>
/// <param name="DeviceType">Type/category of the IoT device.</param>
/// <param name="InstallationDate">Date when the device was installed.</param>
/// <param name="LastCalibration">Date of the most recent calibration.</param>
/// <param name="NextCalibrationDue">Date when the next calibration is required.</param>
/// <param name="DeviceStatus">Current operational status of the device.</param>
/// <param name="Properties">Additional device-specific properties as key-value pairs.</param>
public record DeviceMetadata(
    string DeviceId,
    string FactoryLocation,
    string DeviceType,
    DateTime InstallationDate,
    DateTime LastCalibration,
    DateTime NextCalibrationDue,
    string DeviceStatus,
    Dictionary<string, object> Properties
);
