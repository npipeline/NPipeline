using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Extensions.Nodes.Core.Exceptions;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     A generic filtering node that filters items using one or more predicates.
///     <para>
///     If all predicates pass, the item is forwarded downstream unchanged.
///     If any predicate fails, a <see cref="FilteringException"/> is thrown.
///     The node's error handler decides whether to Skip, Redirect (to dead letter), or Fail the pipeline.
///     </para>
/// </summary>
/// <remarks>
///     <para>
///     Filtering uses exception signalling to integrate cleanly with the existing error-handling/redirect flow
///     used by execution strategies.
///     </para>
///     <para>
///     No allocations on success path other than delegate invocation.
///     Failure path throws FilteringException with optional reason.
///     </para>
/// </remarks>
public sealed class FilteringNode<T> : TransformNode<T, T>
{
    private readonly List<Rule> _rules = [];

    /// <summary>
    ///     Initializes a new instance with optional execution strategy.
    /// </summary>
    public FilteringNode(Execution.IExecutionStrategy? executionStrategy = null)
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
        Execution.IExecutionStrategy? executionStrategy = null) 
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
    ///     Executes filtering on the item.
    ///     Throws <see cref="FilteringException"/> if any predicate fails.
    /// </summary>
    public override Task<T> ExecuteAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Fast-path: no rules configured means pass-through
        if (_rules.Count == 0)
            return Task.FromResult(item);

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

        return Task.FromResult(item);
    }

    private readonly record struct Rule(Func<T, bool> Predicate, Func<T, string>? Reason);
}
