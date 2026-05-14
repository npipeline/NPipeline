using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Provides factory methods for creating resilience policies using a fluent builder pattern.
/// </summary>
public static class ResiliencePolicyBuilder
{
    /// <summary>
    ///     Creates a node-scoped resilience policy builder for item-level failures.
    /// </summary>
    /// <typeparam name="TNode">The node type to target.</typeparam>
    /// <typeparam name="TData">The item type to target.</typeparam>
    /// <returns>A builder for policy rules for the specified node/item pair.</returns>
    public static NodeResiliencePolicyBuilder<TNode, TData> ForNode<TNode, TData>()
        where TNode : INode
    {
        return new NodeResiliencePolicyBuilder<TNode, TData>();
    }

    /// <summary>
    ///     Creates a pre-configured policy that retries item failures up to the specified maximum.
    /// </summary>
    public static IResiliencePolicy RetryAlways<TNode, TData>(int maxRetries = 3)
        where TNode : INode
    {
        return ForNode<TNode, TData>()
            .OnAny().Retry(maxRetries)
            .Build();
    }

    /// <summary>
    ///     Creates a pre-configured policy that retries a specific exception type up to the specified maximum.
    /// </summary>
    public static IResiliencePolicy RetryOn<TNode, TData, TException>(
        int maxRetries = 3,
        ResilienceDecision exhaustedDecision = ResilienceDecision.DeadLetter)
        where TNode : INode
        where TException : Exception
    {
        return ForNode<TNode, TData>()
            .RetryOn<TException>(maxRetries, exhaustedDecision)
            .Build();
    }

    /// <summary>
    ///     Creates a pre-configured policy that always skips matching item failures.
    /// </summary>
    public static IResiliencePolicy SkipAlways<TNode, TData>()
        where TNode : INode
    {
        return ForNode<TNode, TData>()
            .OnAny().Skip()
            .Build();
    }

    /// <summary>
    ///     Creates a pre-configured policy that always dead-letters matching item failures.
    /// </summary>
    public static IResiliencePolicy DeadLetterAlways<TNode, TData>()
        where TNode : INode
    {
        return ForNode<TNode, TData>()
            .OnAny().DeadLetter()
            .Build();
    }
}

/// <summary>
///     Builder for constructing node-scoped resilience policies.
/// </summary>
/// <typeparam name="TNode">Node type to target.</typeparam>
/// <typeparam name="TData">Item type to target.</typeparam>
public sealed class NodeResiliencePolicyBuilder<TNode, TData>
    where TNode : INode
{
    private readonly List<(Predicate<Exception> predicate, Func<int, ResilienceDecision> decisionFactory)> _rules = [];
    private Func<int, ResilienceDecision> _defaultDecision = _ => ResilienceDecision.Fail;

    /// <summary>
    ///     Adds a rule that matches a specific exception type.
    /// </summary>
    public ResilienceRuleBuilder<TNode, TData> On<TException>() where TException : Exception
    {
        return new ResilienceRuleBuilder<TNode, TData>(this, ex => ex is TException);
    }

    /// <summary>
    ///     Adds a catch-all rule that matches any exception.
    /// </summary>
    public ResilienceRuleBuilder<TNode, TData> OnAny()
    {
        return new ResilienceRuleBuilder<TNode, TData>(this, _ => true);
    }

    /// <summary>
    ///     Adds a rule with a custom predicate.
    /// </summary>
    public ResilienceRuleBuilder<TNode, TData> When(Predicate<Exception> predicate)
    {
        return new ResilienceRuleBuilder<TNode, TData>(this, predicate);
    }

    /// <summary>
    ///     Adds a rule that retries a specific exception type for a bounded number of attempts.
    /// </summary>
    public NodeResiliencePolicyBuilder<TNode, TData> RetryOn<TException>(
        int maxRetries = 3,
        ResilienceDecision exhaustedDecision = ResilienceDecision.DeadLetter)
        where TException : Exception
    {
        AddRetryRule(ex => ex is TException, maxRetries, exhaustedDecision);
        return this;
    }

    /// <summary>
    ///     Adds a rule that retries exceptions matching a custom predicate for a bounded number of attempts.
    /// </summary>
    public NodeResiliencePolicyBuilder<TNode, TData> RetryWhen(
        Predicate<Exception> predicate,
        int maxRetries = 3,
        ResilienceDecision exhaustedDecision = ResilienceDecision.DeadLetter)
    {
        AddRetryRule(predicate, maxRetries, exhaustedDecision);
        return this;
    }

    /// <summary>
    ///     Adds a catch-all retry rule for all exception types.
    /// </summary>
    public NodeResiliencePolicyBuilder<TNode, TData> RetryOnAny(
        int maxRetries = 3,
        ResilienceDecision exhaustedDecision = ResilienceDecision.DeadLetter)
    {
        AddRetryRule(_ => true, maxRetries, exhaustedDecision);
        return this;
    }

    internal void AddRule(Predicate<Exception> predicate, Func<int, ResilienceDecision> decisionFactory)
    {
        _rules.Add((predicate, decisionFactory));
    }

    private void AddRetryRule(
        Predicate<Exception> predicate,
        int maxRetries,
        ResilienceDecision exhaustedDecision)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        AddRule(predicate, attempt =>
            attempt <= maxRetries
                ? ResilienceDecision.Retry
                : exhaustedDecision);
    }

    /// <summary>
    ///     Sets the fallback decision when no rule matches.
    /// </summary>
    public NodeResiliencePolicyBuilder<TNode, TData> Otherwise(ResilienceDecision decision)
    {
        _defaultDecision = _ => decision;
        return this;
    }

    /// <summary>
    ///     Builds an <see cref="IResiliencePolicy" /> for the configured rules.
    /// </summary>
    public IResiliencePolicy Build()
    {
        ValidateRuleOrder();
        return new NodeScopedResiliencePolicy<TNode, TData>(_rules.ToList(), _defaultDecision);
    }

    private void ValidateRuleOrder()
    {
        int? catchAllIndex = null;

        for (var i = 0; i < _rules.Count; i++)
        {
            var (predicate, _) = _rules[i];

            if (predicate(new Exception()) &&
                predicate(new InvalidOperationException()) &&
                predicate(new TimeoutException()))
            {
                catchAllIndex = i;
                break;
            }
        }

        if (catchAllIndex.HasValue && catchAllIndex.Value < _rules.Count - 1)
        {
            throw new InvalidOperationException(
                $"A catch-all resilience rule (OnAny) at position {catchAllIndex.Value + 1} is followed by {_rules.Count - catchAllIndex.Value - 1} additional rule(s). " +
                "Catch-all rules must be last because rule evaluation is ordered.");
        }
    }
}

/// <summary>
///     Builder for configuring decisions for a single resilience rule.
/// </summary>
public sealed class ResilienceRuleBuilder<TNode, TData>
    where TNode : INode
{
    private readonly NodeResiliencePolicyBuilder<TNode, TData> _parent;
    private readonly Predicate<Exception> _predicate;

    internal ResilienceRuleBuilder(NodeResiliencePolicyBuilder<TNode, TData> parent, Predicate<Exception> predicate)
    {
        _parent = parent;
        _predicate = predicate;
    }

    /// <summary>
    ///     Retries matching item failures up to the specified maximum.
    /// </summary>
    public NodeResiliencePolicyBuilder<TNode, TData> Retry(int maxRetries = 3)
    {
        _parent.AddRule(_predicate, attempt =>
            attempt <= maxRetries
                ? ResilienceDecision.Retry
                : ResilienceDecision.DeadLetter);

        return _parent;
    }

    /// <summary>
    ///     Skips matching item failures.
    /// </summary>
    public NodeResiliencePolicyBuilder<TNode, TData> Skip()
    {
        _parent.AddRule(_predicate, _ => ResilienceDecision.Skip);
        return _parent;
    }

    /// <summary>
    ///     Dead-letters matching item failures.
    /// </summary>
    public NodeResiliencePolicyBuilder<TNode, TData> DeadLetter()
    {
        _parent.AddRule(_predicate, _ => ResilienceDecision.DeadLetter);
        return _parent;
    }

    /// <summary>
    ///     Fails matching item failures.
    /// </summary>
    public NodeResiliencePolicyBuilder<TNode, TData> Fail()
    {
        _parent.AddRule(_predicate, _ => ResilienceDecision.Fail);
        return _parent;
    }
}

internal sealed class NodeScopedResiliencePolicy<TNode, TData> : ResiliencePolicyBase
    where TNode : INode
{
    private readonly Func<int, ResilienceDecision> _default;
    private readonly List<(Predicate<Exception> predicate, Func<int, ResilienceDecision> decisionFactory)> _rules;

    public NodeScopedResiliencePolicy(
        List<(Predicate<Exception>, Func<int, ResilienceDecision>)> rules,
        Func<int, ResilienceDecision> defaultDecision)
    {
        _rules = rules;
        _default = defaultDecision;
    }

    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeId);

        if (node is not TNode || failedItem is not TData)
            return Task.FromResult(ResilienceDecision.Fail);

        var attempt = retryAttempt + 1;

        foreach (var (predicate, factory) in _rules)
        {
            if (predicate(exception))
                return Task.FromResult(factory(attempt));
        }

        return Task.FromResult(_default(attempt));
    }
}
