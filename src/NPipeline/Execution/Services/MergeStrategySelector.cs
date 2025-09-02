using System.Collections.Concurrent;

namespace NPipeline.Execution.Services;

/// <summary>
///     Default implementation of IMergeStrategySelector that provides typed merge strategies.
/// </summary>
public sealed class MergeStrategySelector : IMergeStrategySelector
{
    private readonly ConcurrentDictionary<(Type DataType, MergeType MergeType), object> _strategyCache = new();

    /// <inheritdoc />
    public object GetStrategy(Type dataType, MergeType mergeType)
    {
        return _strategyCache.GetOrAdd((dataType, mergeType), key =>
        {
            var strategyType = mergeType switch
            {
                MergeType.Interleave => typeof(InterleaveMergeStrategy<>),
                MergeType.Concatenate => typeof(ConcatenateMergeStrategy<>),
                _ => throw new NotSupportedException($"Merge strategy '{mergeType}' is not supported."),
            };

            var genericStrategyType = strategyType.MakeGenericType(key.DataType);
            return Activator.CreateInstance(genericStrategyType)!;
        });
    }
}
