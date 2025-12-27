namespace NPipeline.Execution;

/// <summary>
///     Selects appropriate merge strategies for data pipes based on node definitions and graph configuration.
/// </summary>
public interface IMergeStrategySelector
{
    /// <summary>
    ///     Gets a typed merge strategy for the specified data type and merge type.
    /// </summary>
    /// <param name="dataType">The data type to merge.</param>
    /// <param name="mergeType">The type of merge strategy to use.</param>
    /// <returns>A typed merge strategy instance.</returns>
    object GetStrategy(Type dataType, MergeType mergeType);
}
