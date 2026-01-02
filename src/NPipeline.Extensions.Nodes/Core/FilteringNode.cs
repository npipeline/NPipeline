using System.Linq.Expressions;
using NPipeline.Execution;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     A generic filtering node that filters items using one or more predicates.
///     <para>
///         If all predicates pass, the item is forwarded downstream unchanged.
///         If any predicate fails, a <see cref="FilteringException" /> is thrown.
///         The node's error handler decides whether to Skip, Redirect (to dead letter), or Fail the pipeline.
///     </para>
/// </summary>
/// <remarks>
///     <para>
///         Filtering uses exception signalling to integrate cleanly with the existing error-handling/redirect flow
///         used by execution strategies.
///     </para>
///     <para>
///         No allocations on success path other than delegate invocation.
///         Failure path throws FilteringException with optional reason.
///     </para>
/// </remarks>
public sealed class FilteringNode<T> : TransformNode<T, T>
{
    private readonly List<Rule> _rules = [];

    /// <summary>
    ///     Initializes a new instance with optional execution strategy.
    /// </summary>
    public FilteringNode(IExecutionStrategy? executionStrategy = null)
    {
        if (executionStrategy != null)
            ExecutionStrategy = executionStrategy;
    }

    /// <summary>
    ///     Initializes a new instance with an initial predicate.
    /// </summary>
    /// <param name="predicate">The filtering predicate (return true to pass, false to filter out).</param>
    /// <param name="reason">Optional factory to generate a descriptive message per item on rejection.</param>
    /// <param name="executionStrategy">Optional execution strategy.</param>
    public FilteringNode(
        Func<T, bool> predicate,
        Func<T, string>? reason = null,
        IExecutionStrategy? executionStrategy = null)
        : this(executionStrategy)
    {
        Where(predicate, reason);
    }

    /// <summary>
    ///     Adds a filtering predicate. If it returns false, the item is considered filtered out.
    ///     Optional reason factory can generate a descriptive message per item on rejection.
    /// </summary>
    /// <param name="predicate">The filtering predicate.</param>
    /// <param name="reason">Optional factory for custom rejection reasons.</param>
    /// <returns>This node for method chaining.</returns>
    public FilteringNode<T> Where(Func<T, bool> predicate, Func<T, string>? reason = null)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _rules.Add(new Rule(predicate, reason));
        return this;
    }

    /// <summary>
    ///     Adds a filtering predicate with a fixed reason message.
    /// </summary>
    /// <param name="predicate">The filtering predicate.</param>
    /// <param name="reasonMessage">Fixed rejection reason message.</param>
    /// <returns>This node for method chaining.</returns>
    public FilteringNode<T> Where(Func<T, bool> predicate, string reasonMessage)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(reasonMessage);
        _rules.Add(new Rule(predicate, _ => reasonMessage));
        return this;
    }

    /// <summary>
    ///     Filters items using a property-based predicate.
    /// </summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="propertySelector">Expression selecting the property.</param>
    /// <param name="predicate">Predicate to test the property value.</param>
    /// <param name="reason">Optional rejection reason.</param>
    /// <returns>This node for method chaining.</returns>
    public FilteringNode<T> WhereProperty<TProp>(
        Expression<Func<T, TProp>> propertySelector,
        Func<TProp, bool> predicate,
        string? reason = null)
    {
        ArgumentNullException.ThrowIfNull(propertySelector);
        ArgumentNullException.ThrowIfNull(predicate);

        var accessor = PropertyAccessor.Create(propertySelector);
        var propertyGetter = accessor.Getter;
        var propertyName = accessor.MemberName;
        var defaultReason = $"Property {propertyName} did not meet criteria";

        _rules.Add(new Rule(
            item => predicate(propertyGetter(item)),
            _ => reason ?? defaultReason));

        return this;
    }

    /// <summary>
    ///     Executes filtering on the item.
    ///     Throws <see cref="FilteringException" /> if any predicate fails.
    /// </summary>
    public override Task<T> ExecuteAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
    }

    /// <inheritdoc />
    protected override ValueTask<T> ExecuteValueTaskAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Fast-path: no rules configured means pass-through
        if (_rules.Count == 0)
            return ValueTask.FromResult(item);

        foreach (var rule in _rules)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!rule.Predicate(item))
            {
                // Throw to delegate decision to the configured node-level handler (Skip/Redirect/Fail)
                var message = rule.Reason is not null
                    ? rule.Reason(item)
                    : "Item did not meet filter criteria.";

                throw new FilteringException(message);
            }
        }

        return ValueTask.FromResult(item);
    }

    private readonly record struct Rule(Func<T, bool> Predicate, Func<T, string>? Reason);
}
