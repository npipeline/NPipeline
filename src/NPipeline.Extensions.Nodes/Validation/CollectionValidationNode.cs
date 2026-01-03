using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace NPipeline.Extensions.Nodes;

/// <summary>
///     A validation node for collection properties that validates count, content, and items.
/// </summary>
public sealed class CollectionValidationNode<T> : ValidationNode<T>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="CollectionValidationNode{T}" /> class.
    /// </summary>
    public CollectionValidationNode()
    {
    }

    /// <summary>
    ///     Validates that a collection has a minimum count of items.
    /// </summary>
    public CollectionValidationNode<T> HasMinCount<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        int minCount,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (minCount < 0)
            throw new ArgumentException("Minimum count cannot be negative.", nameof(minCount));

        var ruleName = "HasMinCount";
        var message = errorMessage ?? $"Collection must have at least {minCount} items";
        Register(selector, value => value?.Count() >= minCount, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a collection has a maximum count of items.
    /// </summary>
    public CollectionValidationNode<T> HasMaxCount<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        int maxCount,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (maxCount < 0)
            throw new ArgumentException("Maximum count cannot be negative.", nameof(maxCount));

        var ruleName = "HasMaxCount";
        var message = errorMessage ?? $"Collection must not exceed {maxCount} items";
        Register(selector, value => value?.Count() <= maxCount, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a collection count is within a range.
    /// </summary>
    public CollectionValidationNode<T> HasCountBetween<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        int minCount,
        int maxCount,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (minCount < 0)
            throw new ArgumentException("Minimum count cannot be negative.", nameof(minCount));

        if (maxCount < minCount)
            throw new ArgumentException("Maximum count must be greater than or equal to minimum count.", nameof(maxCount));

        var ruleName = "HasCountBetween";
        var message = errorMessage ?? $"Collection count must be between {minCount} and {maxCount}";

        Register(selector, value =>
        {
            var count = value?.Count() ?? 0;
            return count >= minCount && count <= maxCount;
        }, ruleName, _ => message);

        return this;
    }

    /// <summary>
    ///     Validates that a collection is not empty.
    /// </summary>
    public CollectionValidationNode<T> IsNotEmpty<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNotEmpty";
        var message = errorMessage ?? "Collection must not be empty";
        Register(selector, value => value?.Any() ?? false, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that all items in a collection match a predicate.
    /// </summary>
    public CollectionValidationNode<T> AllMatch<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        Func<TItem, bool> predicate,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(predicate);

        var ruleName = "AllMatch";
        var message = errorMessage ?? "Not all items match the required criteria";
        Register(selector, value => value?.All(predicate) ?? false, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that at least one item in a collection matches a predicate.
    /// </summary>
    public CollectionValidationNode<T> AnyMatch<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        Func<TItem, bool> predicate,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(predicate);

        var ruleName = "AnyMatch";
        var message = errorMessage ?? "At least one item must match the required criteria";
        Register(selector, value => value?.Any(predicate) ?? false, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that no items in a collection match a predicate.
    /// </summary>
    public CollectionValidationNode<T> NoneMatch<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        Func<TItem, bool> predicate,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(predicate);

        var ruleName = "NoneMatch";
        var message = errorMessage ?? "No items should match the criteria";
        Register(selector, value => value?.Any(predicate) != true, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a collection contains a specific item.
    /// </summary>
    public CollectionValidationNode<T> Contains<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        TItem item,
        IEqualityComparer<TItem>? comparer = null,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        var ruleName = "Contains";
        var message = errorMessage ?? "Collection must contain the specified item";
        var equalityComparer = comparer ?? EqualityComparer<TItem>.Default;
        Register(selector, value => value?.Contains(item, equalityComparer) ?? false, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a collection does not contain a specific item.
    /// </summary>
    public CollectionValidationNode<T> DoesNotContain<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        TItem item,
        IEqualityComparer<TItem>? comparer = null,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        var ruleName = "DoesNotContain";
        var message = errorMessage ?? "Collection must not contain the specified item";
        var equalityComparer = comparer ?? EqualityComparer<TItem>.Default;
        Register(selector, value => value?.Contains(item, equalityComparer) != true, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that all items in a collection are unique.
    /// </summary>
    public CollectionValidationNode<T> AllUnique<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        IEqualityComparer<TItem>? comparer = null,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        var ruleName = "AllUnique";
        var message = errorMessage ?? "All collection items must be unique";
        var equalityComparer = comparer ?? EqualityComparer<TItem>.Default;

        Register(selector, value =>
        {
            if (value == null)
                return true;

            var items = value.ToList();
            return items.Count == items.Distinct(equalityComparer).Count();
        }, ruleName, _ => message);

        return this;
    }

    /// <summary>
    ///     Validates that a collection is a subset of allowed values.
    /// </summary>
    public CollectionValidationNode<T> IsSubsetOf<TItem>(
        Expression<Func<T, IEnumerable<TItem>?>> selector,
        IEnumerable<TItem> allowedValues,
        IEqualityComparer<TItem>? comparer = null,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(allowedValues);

        var ruleName = "IsSubsetOf";
        var message = errorMessage ?? "Collection contains items not in the allowed set";
        var equalityComparer = comparer ?? EqualityComparer<TItem>.Default;
        var allowedSet = new HashSet<TItem>(allowedValues, equalityComparer);
        Register(selector, value => value?.All(item => allowedSet.Contains(item)) ?? true, ruleName, _ => message);
        return this;
    }
}
