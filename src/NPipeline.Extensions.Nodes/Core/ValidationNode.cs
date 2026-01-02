using System.Linq.Expressions;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     Base class for property-level validation on items of type <typeparamref name="T" />.
///     Derived classes register validation rules via strongly-typed expressions.
///     Compiled getters provide fast property access with no reflection in hot paths.
///     On validation failure, throws <see cref="ValidationException" />;
///     error handling is controlled by the node's error handler.
/// </summary>
public abstract class ValidationNode<T> : TransformNode<T, T>
{
    private readonly List<IRule<T>> _rules = [];

    /// <summary>
    ///     Registers a validation rule for a specific property/field.
    /// </summary>
    /// <typeparam name="TProp">Property/field type</typeparam>
    /// <param name="selector">Selector like x => x.Email (supports nested)</param>
    /// <param name="predicate">Validation predicate that returns true if valid</param>
    /// <param name="ruleName">Name of the rule for error reporting</param>
    /// <param name="messageFactory">Optional factory to create custom error messages</param>
    /// <returns>This node for method chaining.</returns>
    public ValidationNode<T> Register<TProp>(
        Expression<Func<T, TProp>> selector,
        Func<TProp, bool> predicate,
        string ruleName,
        Func<TProp, string>? messageFactory = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleName);

        var accessor = PropertyAccessor.Create(selector);
        _rules.Add(new Rule<T, TProp>(accessor.MemberName, accessor.Getter, predicate, ruleName, messageFactory));
        return this;
    }

    /// <summary>
    ///     Registers multiple properties with the same validation rule.
    /// </summary>
    /// <typeparam name="TProp">Property/field type</typeparam>
    /// <param name="selectors">Multiple property selectors</param>
    /// <param name="predicate">Validation predicate</param>
    /// <param name="ruleName">Name of the rule</param>
    /// <param name="messageFactory">Optional factory for custom error messages</param>
    /// <returns>This node for method chaining.</returns>
    public ValidationNode<T> RegisterMany<TProp>(
        IEnumerable<Expression<Func<T, TProp>>> selectors,
        Func<TProp, bool> predicate,
        string ruleName,
        Func<TProp, string>? messageFactory = null)
    {
        ArgumentNullException.ThrowIfNull(selectors);

        foreach (var sel in selectors)
        {
            Register(sel, predicate, ruleName, messageFactory);
        }

        return this;
    }

    /// <summary>
    ///     Executes all validation rules on the item asynchronously.
    ///     Throws <see cref="ValidationException" /> if any rule fails.
    /// </summary>
    public override Task<T> ExecuteAsync(
        T item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
    }

    /// <inheritdoc />
    protected override ValueTask<T> ExecuteValueTaskAsync(
        T item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        foreach (var rule in _rules)
        {
            cancellationToken.ThrowIfCancellationRequested();
            rule.Validate(item, context);
        }

        return ValueTask.FromResult(item);
    }

    private interface IRule<in TItem>
    {
        void Validate(TItem item, PipelineContext context);
    }

    private sealed class Rule<TItem, TProp> : IRule<TItem>
    {
        private readonly Func<TItem, TProp> _getter;
        private readonly Func<TProp, string>? _messageFactory;
        private readonly Func<TProp, bool> _predicate;
        private readonly string _propertyPath;
        private readonly string _ruleName;

        public Rule(
            string propertyPath,
            Func<TItem, TProp> getter,
            Func<TProp, bool> predicate,
            string ruleName,
            Func<TProp, string>? messageFactory)
        {
            _propertyPath = propertyPath;
            _getter = getter;
            _predicate = predicate;
            _ruleName = ruleName;
            _messageFactory = messageFactory;
        }

        public void Validate(TItem item, PipelineContext context)
        {
            var value = _getter(item);

            if (!_predicate(value))
            {
                var customMessage = _messageFactory?.Invoke(value);

                var message = customMessage ??
                              $"Validation rule '{_ruleName}' failed for property '{_propertyPath}' with value '{value}'";

                throw new ValidationException(_propertyPath, _ruleName, value, message);
            }
        }
    }
}
