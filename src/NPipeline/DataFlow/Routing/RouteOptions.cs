namespace NPipeline.DataFlow.Routing;

/// <summary>
///     Controls how route conditions are evaluated for a route node.
/// </summary>
public enum RouteMatchMode
{
    /// <summary>
    ///     Routes an item to the first matching route rule only.
    /// </summary>
    FirstMatch,

    /// <summary>
    ///     Routes an item to every matching route rule.
    /// </summary>
    AllMatches,
}

/// <summary>
///     Defines what to do when no route rule matches an item and no otherwise route is configured.
/// </summary>
public enum NoRouteMatchBehavior
{
    /// <summary>
    ///     Drops unmatched items.
    /// </summary>
    Drop,

    /// <summary>
    ///     Throws an exception for unmatched items.
    /// </summary>
    Throw,
}

/// <summary>
///     Well-known output names used by routing helpers.
/// </summary>
public static class RouteOutputNames
{
    /// <summary>
    ///     Default output name used by <c>ConnectOtherwise</c>.
    /// </summary>
    public const string Otherwise = "otherwise";
}

/// <summary>
///     Represents a single named route condition.
/// </summary>
public sealed class RouteRule<T>
{
    public RouteRule(string outputName, Func<T, bool> predicate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outputName);
        ArgumentNullException.ThrowIfNull(predicate);

        OutputName = outputName;
        Predicate = predicate;
    }

    public string OutputName { get; }

    public Func<T, bool> Predicate { get; }
}

/// <summary>
///     Configures conditional routing for a route node.
/// </summary>
public sealed class RouteOptions<T>
{
    private readonly List<RouteRule<T>> _rules = [];

    /// <summary>
    ///     Gets the ordered route rules used to evaluate items.
    /// </summary>
    public IReadOnlyList<RouteRule<T>> Rules => _rules;

    /// <summary>
    ///     Gets the match mode used to resolve route rules.
    /// </summary>
    public RouteMatchMode MatchMode { get; private set; } = RouteMatchMode.FirstMatch;

    /// <summary>
    ///     Gets the behavior for items that do not match any rule and have no otherwise route.
    /// </summary>
    public NoRouteMatchBehavior NoMatchBehavior { get; private set; } = NoRouteMatchBehavior.Drop;

    /// <summary>
    ///     Gets the output name that receives unmatched items, when configured.
    /// </summary>
    public string? OtherwiseOutputName { get; private set; }

    /// <summary>
    ///     Adds a route condition for the specified output.
    /// </summary>
    public RouteOptions<T> When(string outputName, Func<T, bool> predicate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outputName);
        ArgumentNullException.ThrowIfNull(predicate);

        var existingRule = _rules.FirstOrDefault(r => string.Equals(r.OutputName, outputName, StringComparison.Ordinal));

        if (existingRule is not null)
        {
            if (!ReferenceEquals(existingRule.Predicate, predicate))
            {
                throw new InvalidOperationException(
                    $"A route rule for output '{outputName}' is already configured with a different predicate.");
            }

            return this;
        }

        _rules.Add(new RouteRule<T>(outputName, predicate));
        return this;
    }

    /// <summary>
    ///     Routes unmatched items to the specified output.
    /// </summary>
    public RouteOptions<T> Otherwise(string outputName = RouteOutputNames.Otherwise)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outputName);
        OtherwiseOutputName = outputName;
        return this;
    }

    /// <summary>
    ///     Clears any configured otherwise route.
    /// </summary>
    public RouteOptions<T> WithoutOtherwise()
    {
        OtherwiseOutputName = null;
        return this;
    }

    /// <summary>
    ///     Sets how route rules are matched.
    /// </summary>
    public RouteOptions<T> WithMatchMode(RouteMatchMode mode)
    {
        MatchMode = mode;
        return this;
    }

    /// <summary>
    ///     Sets the behavior for unmatched items when no otherwise route exists.
    /// </summary>
    public RouteOptions<T> WithNoMatchBehavior(NoRouteMatchBehavior behavior)
    {
        NoMatchBehavior = behavior;
        return this;
    }
}
