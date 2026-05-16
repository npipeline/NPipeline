namespace NPipeline.Execution;

/// <summary>
///     Runtime stream-item contract for a node after binder normalization.
/// </summary>
public sealed record RuntimeNodeStreamContract(
    Type? EffectiveInputItemType,
    Type? EffectiveOutputItemType,
    bool ItemLevelLineageEnabled);
