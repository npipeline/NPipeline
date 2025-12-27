using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Provides factory methods for creating error handlers using a fluent builder pattern.
/// </summary>
/// <remarks>
///     <para>
///         This class offers a fluent API for building error handlers without needing to implement
///         the <see cref="INodeErrorHandler{TNode,TData}" /> interface manually. It's particularly useful
///         for simple error handling scenarios where creating a dedicated handler class would be overly verbose.
///     </para>
///     <para>
///         The fluent builder allows you to define error handling rules based on exception types
///         and predicates, and specify the decision (Retry, Skip, DeadLetter, Fail) for each rule.
///     </para>
///     <example>
///         <code>
/// // Create a handler that retries on timeout, skips on validation errors
/// var handler = ErrorHandler.ForNode&lt;MyTransform, string&gt;()
///     .On&lt;TimeoutException&gt;().Retry(3)
///     .On&lt;ValidationException&gt;().Skip()
///     .OnAny().DeadLetter()
///     .Build();
/// </code>
///     </example>
/// </remarks>
public static class ErrorHandler
{
    /// <summary>
    ///     Creates a new error handler builder for node-level errors.
    /// </summary>
    /// <typeparam name="TNode">The type of node this handler will be used with.</typeparam>
    /// <typeparam name="TData">The type of data items processed by the node.</typeparam>
    /// <returns>A builder for configuring the error handler.</returns>
    /// <remarks>
    ///     Use this method to start building a custom error handler. The builder provides
    ///     methods to define rules based on exception types and specify appropriate recovery actions.
    /// </remarks>
    /// <example>
    ///     <code>
    /// var handler = ErrorHandler.ForNode&lt;MyTransform, string&gt;()
    ///     .On&lt;TimeoutException&gt;().Retry(3)
    ///     .On&lt;ValidationException&gt;().Skip()
    ///     .OnAny().DeadLetter()
    ///     .Build();
    /// </code>
    /// </example>
    public static NodeErrorHandlerBuilder<TNode, TData> ForNode<TNode, TData>()
        where TNode : INode
    {
        return new NodeErrorHandlerBuilder<TNode, TData>();
    }

    /// <summary>
    ///     Creates a pre-configured handler that always retries failed items.
    /// </summary>
    /// <typeparam name="TNode">The type of node this handler will be used with.</typeparam>
    /// <typeparam name="TData">The type of data items processed by the node.</typeparam>
    /// <param name="maxRetries">The maximum number of retry attempts. Default is 3.</param>
    /// <returns>An error handler that retries up to the specified maximum before dead-lettering.</returns>
    /// <remarks>
    ///     This is a convenience method for creating a simple retry handler. After the maximum
    ///     number of retries is exhausted, the item will be sent to the dead-letter sink.
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Retry up to 5 times on any error
    /// var handler = ErrorHandler.RetryAlways&lt;MyTransform, string&gt;(5);
    /// </code>
    /// </example>
    public static INodeErrorHandler RetryAlways<TNode, TData>(int maxRetries = 3)
        where TNode : INode
    {
        return ForNode<TNode, TData>()
            .OnAny().Retry(maxRetries)
            .Build();
    }

    /// <summary>
    ///     Creates a pre-configured handler that always skips failed items.
    /// </summary>
    /// <typeparam name="TNode">The type of node this handler will be used with.</typeparam>
    /// <typeparam name="TData">The type of data items processed by the node.</typeparam>
    /// <returns>An error handler that skips all failed items.</returns>
    /// <remarks>
    ///     This is useful for scenarios where you want to continue processing even when
    ///     some items fail, such as best-effort data processing pipelines.
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Skip any items that cause errors
    /// var handler = ErrorHandler.SkipAlways&lt;MyTransform, string&gt;();
    /// </code>
    /// </example>
    public static INodeErrorHandler SkipAlways<TNode, TData>()
        where TNode : INode
    {
        return ForNode<TNode, TData>()
            .OnAny().Skip()
            .Build();
    }

    /// <summary>
    ///     Creates a pre-configured handler that always sends failed items to the dead-letter sink.
    /// </summary>
    /// <typeparam name="TNode">The type of node this handler will be used with.</typeparam>
    /// <typeparam name="TData">The type of data items processed by the node.</typeparam>
    /// <returns>An error handler that dead-letters all failed items.</returns>
    /// <remarks>
    ///     This is useful when you want to capture all failed items for later analysis
    ///     and reprocessing without halting the pipeline.
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Send all errors to dead-letter sink
    /// var handler = ErrorHandler.DeadLetterAlways&lt;MyTransform, string&gt;();
    /// </code>
    /// </example>
    public static INodeErrorHandler DeadLetterAlways<TNode, TData>()
        where TNode : INode
    {
        return ForNode<TNode, TData>()
            .OnAny().DeadLetter()
            .Build();
    }
}

/// <summary>
///     Builder for constructing node error handlers using a fluent API.
/// </summary>
/// <typeparam name="TNode">The type of node this handler will be used with.</typeparam>
/// <typeparam name="TData">The type of data items processed by the node.</typeparam>
/// <remarks>
///     This builder allows you to define error handling rules by specifying:
///     <list type="bullet">
///         <item>Which exceptions to match (using On, OnAny, or When methods)</item>
///         <item>What action to take (Retry, Skip, DeadLetter, or Fail)</item>
///         <item>A default action for exceptions that don't match any rule</item>
///     </list>
///     Rules are evaluated in the order they are added. The first matching rule determines the action.
/// </remarks>
public sealed class NodeErrorHandlerBuilder<TNode, TData>
    where TNode : INode
{
    private readonly List<(Predicate<Exception> predicate, Func<int, NodeErrorDecision> decisionFactory)> _rules = [];
    private Func<int, NodeErrorDecision> _defaultDecision = _ => NodeErrorDecision.Fail;

    /// <summary>
    ///     Adds a rule that matches a specific exception type.
    /// </summary>
    /// <typeparam name="TException">The type of exception to match.</typeparam>
    /// <returns>A rule builder for specifying the action to take when this exception is caught.</returns>
    /// <remarks>
    ///     The rule will match the specified exception type and any derived types.
    /// </remarks>
    /// <example>
    ///     <code>
    /// builder.On&lt;TimeoutException&gt;().Retry(3);
    /// </code>
    /// </example>
    public ErrorRuleBuilder<TNode, TData> On<TException>() where TException : Exception
    {
        return new ErrorRuleBuilder<TNode, TData>(this, ex => ex is TException);
    }

    /// <summary>
    ///     Adds a catch-all rule that matches any exception not caught by previous rules.
    /// </summary>
    /// <returns>A rule builder for specifying the action to take.</returns>
    /// <remarks>
    ///     <para>
    ///         This method creates a rule with a predicate that matches any exception.
    ///         Because rules are evaluated in order, only use this as the <strong>last rule</strong>
    ///         to define a catch-all behavior for exceptions that don't match earlier rules.
    ///     </para>
    ///     <para>
    ///         Placing <c>OnAny()</c> before other rules is typically incorrect because it will
    ///         prevent those subsequent rules from ever matching (since all exceptions match <c>OnAny()</c>).
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// builder.On&lt;TimeoutException&gt;().Retry(3)
    ///        .On&lt;ValidationException&gt;().Skip()
    ///        .OnAny().DeadLetter();  // Catch-all for unmatched exceptions
    /// </code>
    /// </example>
    public ErrorRuleBuilder<TNode, TData> OnAny()
    {
        return new ErrorRuleBuilder<TNode, TData>(this, _ => true);
    }

    /// <summary>
    ///     Adds a rule using a custom predicate to match exceptions.
    /// </summary>
    /// <param name="predicate">A function that returns true if the exception should be handled by this rule.</param>
    /// <returns>A rule builder for specifying the action to take.</returns>
    /// <remarks>
    ///     This allows for complex matching logic, such as checking exception messages or properties.
    /// </remarks>
    /// <example>
    ///     <code>
    /// builder.When(ex => ex.Message.Contains("timeout")).Retry(3);
    /// </code>
    /// </example>
    public ErrorRuleBuilder<TNode, TData> When(Predicate<Exception> predicate)
    {
        return new ErrorRuleBuilder<TNode, TData>(this, predicate);
    }

    internal void AddRule(Predicate<Exception> predicate, Func<int, NodeErrorDecision> decisionFactory)
    {
        _rules.Add((predicate, decisionFactory));
    }

    /// <summary>
    ///     Sets the default decision to use when no rules match the exception.
    /// </summary>
    /// <param name="decision">The decision to use as a fallback.</param>
    /// <returns>This builder instance for method chaining.</returns>
    /// <remarks>
    ///     If not specified, the default is <see cref="NodeErrorDecision.Fail" />.
    /// </remarks>
    /// <example>
    ///     <code>
    /// builder
    ///     .On&lt;TimeoutException&gt;().Retry(3)
    ///     .Otherwise(NodeErrorDecision.DeadLetter);
    /// </code>
    /// </example>
    public NodeErrorHandlerBuilder<TNode, TData> Otherwise(NodeErrorDecision decision)
    {
        _defaultDecision = _ => decision;
        return this;
    }

    /// <summary>
    ///     Builds the error handler from the configured rules.
    /// </summary>
    /// <returns>An instance of <see cref="INodeErrorHandler" /> that implements the specified rules.</returns>
    /// <remarks>
    ///     <para>
    ///         Once built, the handler can be used with a pipeline node to handle errors during execution.
    ///     </para>
    ///     <para>
    ///         <strong>Runtime Validation:</strong> This method validates that if any rule uses a catch-all predicate
    ///         (created by <see cref="OnAny()" />), it is the last rule. A catch-all rule placed before other rules
    ///         will prevent those subsequent rules from matching. If such a configuration is detected, an
    ///         <see cref="InvalidOperationException" /> is thrown with a helpful message.
    ///     </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when a catch-all rule (<see cref="OnAny()" />) is not positioned as the last rule,
    ///     as it would make all subsequent rules unreachable.
    /// </exception>
    /// <example>
    ///     <code>
    /// var handler = ErrorHandler.ForNode&lt;MyTransform, string&gt;()
    ///     .On&lt;TimeoutException&gt;().Retry(3)
    ///     .On&lt;ValidationException&gt;().Skip()
    ///     .OnAny().DeadLetter()
    ///     .Build();
    /// </code>
    /// </example>
    public INodeErrorHandler Build()
    {
        ValidateRuleOrder();
        return new FluentNodeErrorHandler<TNode, TData>(_rules.ToList(), _defaultDecision);
    }

    /// <summary>
    ///     Validates that catch-all rules are not positioned before other rules.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when a catch-all rule (predicate that always returns true) is found before other rules.
    /// </exception>
    private void ValidateRuleOrder()
    {
        // Find the index of the last catch-all rule (always-true predicate)
        int? catchAllIndex = null;

        for (var i = 0; i < _rules.Count; i++)
        {
            var (predicate, _) = _rules[i];

            // Test with a few different exception types to detect catch-all predicates
            if (predicate(new Exception()) &&
                predicate(new InvalidOperationException()) &&
                predicate(new TimeoutException()))
            {
                catchAllIndex = i;

                // Don't break - we want to find the LAST catch-all to report the earliest problem
                break;
            }
        }

        // If we found a catch-all and it's not the last rule, throw
        if (catchAllIndex.HasValue && catchAllIndex.Value < _rules.Count - 1)
        {
            throw new InvalidOperationException(
                $"A catch-all error rule (OnAny) at position {catchAllIndex.Value + 1} is followed by {_rules.Count - catchAllIndex.Value - 1} additional rule(s). " +
                $"Catch-all rules should be placed last, as rules are evaluated in order and a matching catch-all prevents subsequent rules from executing. " +
                $"Reorder your rules to place catch-all rules at the end of the builder chain. " +
                $"Example: builder.On<SpecificException>().Retry().OnAny().DeadLetter().Build()");
        }
    }
}

/// <summary>
///     Builder for specifying the action to take for a specific error rule.
/// </summary>
/// <typeparam name="TNode">The type of node this handler will be used with.</typeparam>
/// <typeparam name="TData">The type of data items processed by the node.</typeparam>
public sealed class ErrorRuleBuilder<TNode, TData>
    where TNode : INode
{
    private readonly NodeErrorHandlerBuilder<TNode, TData> _parent;
    private readonly Predicate<Exception> _predicate;

    internal ErrorRuleBuilder(NodeErrorHandlerBuilder<TNode, TData> parent, Predicate<Exception> predicate)
    {
        _parent = parent;
        _predicate = predicate;
    }

    /// <summary>
    ///     Configures this rule to retry the operation up to the specified maximum.
    /// </summary>
    /// <param name="maxRetries">The maximum number of retry attempts. Default is 3.</param>
    /// <returns>The parent builder for chaining additional rules.</returns>
    /// <remarks>
    ///     <para>
    ///         When this rule matches, the pipeline will retry the failed item up to maxRetries times.
    ///         If all retries are exhausted, the item will be sent to the dead-letter sink.
    ///     </para>
    ///     <para>
    ///         The retry count is tracked per item. Each time this handler is called for the same item,
    ///         the attempt count increments until maxRetries is reached.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// builder.On&lt;TimeoutException&gt;().Retry(3);
    /// </code>
    /// </example>
    public NodeErrorHandlerBuilder<TNode, TData> Retry(int maxRetries = 3)
    {
        _parent.AddRule(_predicate, attempt =>
            attempt <= maxRetries
                ? NodeErrorDecision.Retry
                : NodeErrorDecision.DeadLetter);

        return _parent;
    }

    /// <summary>
    ///     Configures this rule to skip the failed item and continue processing.
    /// </summary>
    /// <returns>The parent builder for chaining additional rules.</returns>
    /// <remarks>
    ///     When this rule matches, the pipeline will skip the failed item and continue with the next item.
    ///     The skipped item is lost unless you also have a dead-letter sink configured.
    /// </remarks>
    /// <example>
    ///     <code>
    /// builder.On&lt;ValidationException&gt;().Skip();
    /// </code>
    /// </example>
    public NodeErrorHandlerBuilder<TNode, TData> Skip()
    {
        _parent.AddRule(_predicate, _ => NodeErrorDecision.Skip);
        return _parent;
    }

    /// <summary>
    ///     Configures this rule to send the failed item to the dead-letter sink.
    /// </summary>
    /// <returns>The parent builder for chaining additional rules.</returns>
    /// <remarks>
    ///     When this rule matches, the pipeline will send the failed item to the configured
    ///     dead-letter sink for later analysis or reprocessing, then continue with the next item.
    /// </remarks>
    /// <example>
    ///     <code>
    /// builder.On&lt;DataFormatException&gt;().DeadLetter();
    /// </code>
    /// </example>
    public NodeErrorHandlerBuilder<TNode, TData> DeadLetter()
    {
        _parent.AddRule(_predicate, _ => NodeErrorDecision.DeadLetter);
        return _parent;
    }

    /// <summary>
    ///     Configures this rule to fail the entire pipeline.
    /// </summary>
    /// <returns>The parent builder for chaining additional rules.</returns>
    /// <remarks>
    ///     When this rule matches, the pipeline will stop processing and propagate the error.
    ///     Use this for critical errors that should halt the entire pipeline.
    /// </remarks>
    /// <example>
    ///     <code>
    /// builder.On&lt;SecurityException&gt;().Fail();
    /// </code>
    /// </example>
    public NodeErrorHandlerBuilder<TNode, TData> Fail()
    {
        _parent.AddRule(_predicate, _ => NodeErrorDecision.Fail);
        return _parent;
    }
}

/// <summary>
///     Internal implementation of INodeErrorHandler that executes the rules defined by the fluent builder.
/// </summary>
/// <typeparam name="TNode">The type of node this handler is used with.</typeparam>
/// <typeparam name="TData">The type of data items processed by the node.</typeparam>
internal sealed class FluentNodeErrorHandler<TNode, TData> : INodeErrorHandler<TNode, TData>
    where TNode : INode
{
    private readonly Func<int, NodeErrorDecision> _default;
    private readonly List<(Predicate<Exception> predicate, Func<int, NodeErrorDecision> decisionFactory)> _rules;
    private int _attemptCount;

    public FluentNodeErrorHandler(
        List<(Predicate<Exception>, Func<int, NodeErrorDecision>)> rules,
        Func<int, NodeErrorDecision> defaultDecision)
    {
        _rules = rules;
        _default = defaultDecision;
    }

    public Task<NodeErrorDecision> HandleAsync(
        TNode node,
        TData failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _attemptCount++;

        foreach (var (predicate, factory) in _rules)
        {
            if (predicate(error))
            {
                var decision = factory(_attemptCount);

                if (decision != NodeErrorDecision.Retry)
                    _attemptCount = 0; // Reset for next item

                return Task.FromResult(decision);
            }
        }

        var defaultDecision = _default(_attemptCount);

        if (defaultDecision != NodeErrorDecision.Retry)
            _attemptCount = 0; // Reset for next item

        return Task.FromResult(defaultDecision);
    }
}
