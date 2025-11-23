namespace Sample_LookupNode.Models;

/// <summary>
///     Represents the risk level associated with a sensor reading or device status.
///     This enum is used to prioritize alerts and monitoring actions.
/// </summary>
public enum RiskLevel
{
    /// <summary>
    ///     No significant risk detected.
    /// </summary>
    Low,

    /// <summary>
    ///     Minor risk that should be monitored.
    /// </summary>
    Medium,

    /// <summary>
    ///     Significant risk requiring attention.
    /// </summary>
    High,

    /// <summary>
    ///     Critical risk requiring immediate action.
    /// </summary>
    Critical,
}
