using System.Linq.Expressions;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
/// A cleansing node for collection properties that provides operations for normalization and transformation.
/// </summary>
public sealed class CollectionCleansingNode<T> : PropertyTransformationNode<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CollectionCleansingNode{T}" /> class.
    /// </summary>
    public CollectionCleansingNode()
    {
    }

    /// <summary>
    /// Removes null entries from a collection.
    /// </summary>
    public CollectionCleansingNode<T> RemoveNulls<TItem>(Expression<Func<T, IEnumerable<TItem?>>> selector)
        where TItem : class
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value.Where(item => item != null).ToList()!);
        return this;
    }

    /// <summary>
    /// Removes duplicate entries from a collection.
    /// </summary>
    public CollectionCleansingNode<T> RemoveDuplicates<TItem>(
        Expression<Func<T, IEnumerable<TItem>>> selector,
        IEqualityComparer<TItem>? comparer = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var equalityComparer = comparer ?? EqualityComparer<TItem>.Default;
        Register(selector, value => value.Distinct(equalityComparer).ToList());
        return this;
    }

    /// <summary>
    /// Removes empty strings from a string collection.
    /// </summary>
    public CollectionCleansingNode<T> RemoveEmpty(Expression<Func<T, IEnumerable<string>>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value.Where(s => !string.IsNullOrEmpty(s)).ToList());
        return this;
    }

    /// <summary>
    /// Removes whitespace-only strings from a string collection.
    /// </summary>
    public CollectionCleansingNode<T> RemoveWhitespace(Expression<Func<T, IEnumerable<string>>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value.Where(s => !string.IsNullOrWhiteSpace(s)).ToList());
        return this;
    }

    /// <summary>
    /// Sorts a collection in ascending order.
    /// </summary>
    public CollectionCleansingNode<T> Sort<TItem>(
        Expression<Func<T, IEnumerable<TItem>>> selector,
        IComparer<TItem>? comparer = null)
        where TItem : IComparable<TItem>
    {
        ArgumentNullException.ThrowIfNull(selector);
        var itemComparer = comparer ?? Comparer<TItem>.Default;
        Register(selector, value => value.OrderBy(x => x, itemComparer).ToList());
        return this;
    }

    /// <summary>
    /// Reverses the order of items in a collection.
    /// </summary>
    public CollectionCleansingNode<T> Reverse<TItem>(Expression<Func<T, IEnumerable<TItem>>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value.Reverse().ToList());
        return this;
    }

    /// <summary>
    /// Takes the first N items from a collection.
    /// </summary>
    public CollectionCleansingNode<T> Take<TItem>(Expression<Func<T, IEnumerable<TItem>>> selector, int count)
    {
        ArgumentNullException.ThrowIfNull(selector);
        if (count < 0)
            throw new ArgumentException("Count cannot be negative.", nameof(count));

        Register(selector, value => value.Take(count).ToList());
        return this;
    }

    /// <summary>
    /// Skips the first N items in a collection.
    /// </summary>
    public CollectionCleansingNode<T> Skip<TItem>(Expression<Func<T, IEnumerable<TItem>>> selector, int count)
    {
        ArgumentNullException.ThrowIfNull(selector);
        if (count < 0)
            throw new ArgumentException("Count cannot be negative.", nameof(count));

        Register(selector, value => value.Skip(count).ToList());
        return this;
    }
}
