using NPipeline.DataFlow.Routing;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Decisions;

/// <summary>Builds confidence-aware branches over typed AI classifications.</summary>
public sealed class AIRouteBuilder<TInput, TLabel> : IInputNodeHandle<TInput>
    where TLabel : notnull
{
    private readonly PipelineBuilder _builder;
    private int _branchIndex;
    private bool _hasOtherwise;

    internal AIRouteBuilder(
        PipelineBuilder builder,
        TransformNodeHandle<TInput, AIClassifiedItem<TInput, TLabel>> classificationHandle,
        TransformNodeHandle<AIClassifiedItem<TInput, TLabel>, AIClassifiedItem<TInput, TLabel>> routeHandle,
        string baseName)
    {
        _builder = builder;
        ClassificationHandle = classificationHandle;
        RouteHandle = routeHandle;
        BaseName = baseName;
    }

    /// <summary>Gets the classification node handle for advanced envelope-based connections.</summary>
    public TransformNodeHandle<TInput, AIClassifiedItem<TInput, TLabel>> ClassificationHandle { get; }

    /// <summary>Gets the internal route node handle.</summary>
    public TransformNodeHandle<AIClassifiedItem<TInput, TLabel>, AIClassifiedItem<TInput, TLabel>> RouteHandle { get; }

    private string BaseName { get; }

    string INodeHandle.Id => ClassificationHandle.Id;

    /// <summary>Sets whether the first matching branch or every matching branch receives an item.</summary>
    public AIRouteBuilder<TInput, TLabel> WithMatchMode(RouteMatchMode mode)
    {
        _builder.ConfigureRoute(RouteHandle, options => options.WithMatchMode(mode));
        return this;
    }

    /// <summary>Routes the selected label when its confidence meets the minimum.</summary>
    public AIRouteBuilder<TInput, TLabel> WhenLabel(
        TLabel label,
        IInputNodeHandle<TInput> target,
        double minimumConfidence = 0)
    {
        ValidateThreshold(minimumConfidence, nameof(minimumConfidence));
        var comparer = EqualityComparer<TLabel>.Default;

        return When(
            classification => comparer.Equals(classification.Label, label) && classification.Confidence >= minimumConfidence,
            target);
    }

    /// <summary>Routes an item when a label's probability meets the minimum.</summary>
    public AIRouteBuilder<TInput, TLabel> WhenProbability(
        TLabel label,
        double minimumProbability,
        IInputNodeHandle<TInput> target)
    {
        ValidateThreshold(minimumProbability, nameof(minimumProbability));

        return When(
            classification => classification.Probabilities.TryGetValue(label, out var probability) && probability >= minimumProbability,
            target);
    }

    /// <summary>Routes an item when the classification predicate succeeds.</summary>
    public AIRouteBuilder<TInput, TLabel> When(
        Func<AIClassification<TLabel>, bool> predicate,
        IInputNodeHandle<TInput> target)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(target);

        var branch = AddUnwrapBranch(target);

        _builder.ConnectWhen(
            RouteHandle,
            branch,
            classified => predicate(classified.Classification),
            $"{BaseName}_decision_{_branchIndex}");

        return this;
    }

    /// <summary>Routes items that did not match another branch.</summary>
    public AIRouteBuilder<TInput, TLabel> Otherwise(IInputNodeHandle<TInput> target)
    {
        ArgumentNullException.ThrowIfNull(target);

        if (_hasOtherwise)
            throw new InvalidOperationException("An otherwise branch is already configured.");

        _hasOtherwise = true;
        var branch = AddUnwrapBranch(target);
        _builder.ConnectOtherwise(RouteHandle, branch);
        return this;
    }

    private TransformNodeHandle<AIClassifiedItem<TInput, TLabel>, TInput> AddUnwrapBranch(IInputNodeHandle<TInput> target)
    {
        var branchIndex = ++_branchIndex;

        var unwrap = _builder.AddTransform(
            (AIClassifiedItem<TInput, TLabel> classified) => classified.Item,
            $"{BaseName}_unwrap_{branchIndex}");

        _builder.Connect(unwrap, target);
        return unwrap;
    }

    private static void ValidateThreshold(double threshold, string parameterName)
    {
        if (!double.IsFinite(threshold) || threshold is < 0 or > 1)
            throw new ArgumentOutOfRangeException(parameterName, threshold, "Threshold must be a finite value from 0 to 1.");
    }
}
