namespace Sample_LookupNode.Models;

/// <summary>
///     Represents the operational status of an IoT device.
///     This enum is used to determine device availability and maintenance needs.
/// </summary>
public enum DeviceStatus
{
    /// <summary>
    ///     Device is online and functioning normally.
    /// </summary>
    Online,

    /// <summary>
    ///     Device is offline and not reporting data.
    /// </summary>
    Offline,

    /// <summary>
    ///     Device is under maintenance and temporarily unavailable.
    /// </summary>
    Maintenance,

    /// <summary>
    ///     Device requires immediate calibration.
    /// </summary>
    CalibrationRequired,

    /// <summary>
    ///     Device has reported an error condition.
    /// </summary>
    Error,
}
