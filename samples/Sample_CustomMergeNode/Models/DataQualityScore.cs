using System;

namespace Sample_CustomMergeNode.Models;

/// <summary>
///     Represents a data quality score for market data
/// </summary>
public class DataQualityScore
{
    /// <summary>
    ///     Initializes a new instance of the DataQualityScore class
    /// </summary>
    /// <param name="completeness">Completeness score (0-100)</param>
    /// <param name="timeliness">Timeliness score (0-100)</param>
    /// <param name="accuracy">Accuracy score (0-100)</param>
    /// <param name="consistency">Consistency score (0-100)</param>
    public DataQualityScore(double completeness, double timeliness, double accuracy, double consistency)
    {
        Completeness = Math.Max(0, Math.Min(100, completeness));
        Timeliness = Math.Max(0, Math.Min(100, timeliness));
        Accuracy = Math.Max(0, Math.Min(100, accuracy));
        Consistency = Math.Max(0, Math.Min(100, consistency));
    }

    /// <summary>
    ///     Gets the completeness score (0-100)
    /// </summary>
    public double Completeness { get; }

    /// <summary>
    ///     Gets the timeliness score (0-100)
    /// </summary>
    public double Timeliness { get; }

    /// <summary>
    ///     Gets the accuracy score (0-100)
    /// </summary>
    public double Accuracy { get; }

    /// <summary>
    ///     Gets the consistency score (0-100)
    /// </summary>
    public double Consistency { get; }

    /// <summary>
    ///     Gets the overall quality score (0-100)
    /// </summary>
    public double OverallScore => (Completeness + Timeliness + Accuracy + Consistency) / 4.0;

    /// <summary>
    ///     Returns a string representation of the data quality score
    /// </summary>
    /// <returns>String representation</returns>
    public override string ToString()
    {
        return $"Q:{OverallScore:F1} [C:{Completeness:F1},T:{Timeliness:F1},A:{Accuracy:F1},S:{Consistency:F1}]";
    }

    /// <summary>
    ///     Determines if the quality score is acceptable
    /// </summary>
    /// <param name="threshold">Minimum acceptable overall score</param>
    /// <returns>True if acceptable, false otherwise</returns>
    public bool IsAcceptable(double threshold = 70.0)
    {
        return OverallScore >= threshold;
    }

    /// <summary>
    ///     Multiplies the quality score by a factor
    /// </summary>
    /// <param name="score">The quality score</param>
    /// <param name="factor">The multiplication factor</param>
    /// <returns>The multiplied quality score</returns>
    public static DataQualityScore operator *(DataQualityScore score, double factor)
    {
        return new DataQualityScore(
            score.Completeness * factor,
            score.Timeliness * factor,
            score.Accuracy * factor,
            score.Consistency * factor
        );
    }
}
