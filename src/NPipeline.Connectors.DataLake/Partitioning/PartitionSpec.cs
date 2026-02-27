using System.Linq.Expressions;
using System.Text;

namespace NPipeline.Connectors.DataLake.Partitioning;

/// <summary>
///     Fluent builder describing how records are routed into Hive-style partition directories.
///     Example: <c>PartitionSpec&lt;T&gt;.By(x => x.EventDate).ThenBy(x => x.Region)</c>
///     produces paths like: <c>event_date=2025-01-15/region=EU/</c>
/// </summary>
/// <typeparam name="T">The record type being partitioned.</typeparam>
public sealed class PartitionSpec<T>
{
    private readonly List<PartitionColumn<T>> _columns = [];

    private PartitionSpec()
    {
    }

    /// <summary>
    ///     Gets the ordered list of partition columns.
    /// </summary>
    public IReadOnlyList<PartitionColumn<T>> Columns => _columns;

    /// <summary>
    ///     Gets a value indicating whether this spec has any partition columns.
    /// </summary>
    public bool HasPartitions => _columns.Count > 0;

    /// <summary>
    ///     Starts building a partition spec with the first partition column.
    /// </summary>
    /// <typeparam name="TProperty">The type of the partition property.</typeparam>
    /// <param name="propertyExpression">Expression selecting the partition property.</param>
    /// <returns>A new partition spec with the first column added.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="propertyExpression" /> is <c>null</c>.</exception>
#pragma warning disable CA1000 // Do not declare static members on generic types - fluent builder pattern requires it
    public static PartitionSpec<T> By<TProperty>(Expression<Func<T, TProperty>> propertyExpression)
#pragma warning restore CA1000
    {
        ArgumentNullException.ThrowIfNull(propertyExpression);
        var spec = new PartitionSpec<T>();
        spec._columns.Add(CreatePartitionColumn(propertyExpression));
        return spec;
    }

    /// <summary>
    ///     Starts building a partition spec with the first partition column using a custom column name.
    /// </summary>
    /// <typeparam name="TProperty">The type of the partition property.</typeparam>
    /// <param name="propertyExpression">Expression selecting the partition property.</param>
    /// <param name="columnName">The custom column name to use in the partition path.</param>
    /// <returns>A new partition spec with the first column added.</returns>
    /// <exception cref="ArgumentNullException">
    ///     Thrown if <paramref name="propertyExpression" /> or <paramref name="columnName" /> is <c>null</c>.
    /// </exception>
#pragma warning disable CA1000 // Do not declare static members on generic types - fluent builder pattern requires it
    public static PartitionSpec<T> By<TProperty>(
#pragma warning restore CA1000
        Expression<Func<T, TProperty>> propertyExpression,
        string columnName)
    {
        ArgumentNullException.ThrowIfNull(propertyExpression);
        ArgumentNullException.ThrowIfNull(columnName);
        var spec = new PartitionSpec<T>();
        spec._columns.Add(CreatePartitionColumn(propertyExpression, columnName));
        return spec;
    }

    /// <summary>
    ///     Adds another partition column to the spec.
    /// </summary>
    /// <typeparam name="TProperty">The type of the partition property.</typeparam>
    /// <param name="propertyExpression">Expression selecting the partition property.</param>
    /// <returns>This partition spec with the additional column added.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="propertyExpression" /> is <c>null</c>.</exception>
    public PartitionSpec<T> ThenBy<TProperty>(Expression<Func<T, TProperty>> propertyExpression)
    {
        ArgumentNullException.ThrowIfNull(propertyExpression);
        _columns.Add(CreatePartitionColumn(propertyExpression));
        return this;
    }

    /// <summary>
    ///     Adds another partition column to the spec with a custom column name.
    /// </summary>
    /// <typeparam name="TProperty">The type of the partition property.</typeparam>
    /// <param name="propertyExpression">Expression selecting the partition property.</param>
    /// <param name="columnName">The custom column name to use in the partition path.</param>
    /// <returns>This partition spec with the additional column added.</returns>
    /// <exception cref="ArgumentNullException">
    ///     Thrown if <paramref name="propertyExpression" /> or <paramref name="columnName" /> is <c>null</c>.
    /// </exception>
    public PartitionSpec<T> ThenBy<TProperty>(
        Expression<Func<T, TProperty>> propertyExpression,
        string columnName)
    {
        ArgumentNullException.ThrowIfNull(propertyExpression);
        ArgumentNullException.ThrowIfNull(columnName);
        _columns.Add(CreatePartitionColumn(propertyExpression, columnName));
        return this;
    }

    /// <summary>
    ///     Creates an empty partition spec (no partitioning).
    /// </summary>
    /// <returns>An empty partition spec.</returns>
#pragma warning disable CA1000 // Do not declare static members on generic types - fluent builder pattern requires it
    public static PartitionSpec<T> None()
#pragma warning restore CA1000
    {
        return new PartitionSpec<T>();
    }

    private static PartitionColumn<T> CreatePartitionColumn<TProperty>(
        Expression<Func<T, TProperty>> propertyExpression,
        string? columnName = null)
    {
        var memberExpression = GetMemberExpression(propertyExpression);
        var propertyName = memberExpression.Member.Name;
        var name = columnName ?? ConvertToSnakeCase(propertyName);
        var compiledGetter = propertyExpression.Compile();

        return new PartitionColumn<T>
        {
            PropertyName = propertyName,
            ColumnName = name,
            ValueType = typeof(TProperty),
            GetValue = record => compiledGetter(record!),
        };
    }

    private static MemberExpression GetMemberExpression<TProperty>(Expression<Func<T, TProperty>> expression)
    {
        return expression.Body switch
        {
            MemberExpression memberExpression => memberExpression,
            UnaryExpression { Operand: MemberExpression unaryMemberExpression } => unaryMemberExpression,
            _ => throw new ArgumentException(
                $"Expression '{expression}' must refer to a property.",
                nameof(expression)),
        };
    }

    private static string ConvertToSnakeCase(string propertyName)
    {
        // Convert PascalCase to snake_case (e.g., EventDate -> event_date)
        var span = propertyName.AsSpan();
        var result = new StringBuilder(span.Length + 5);

        for (var i = 0; i < span.Length; i++)
        {
            var c = span[i];

            if (char.IsUpper(c))
            {
                if (i > 0)
                    _ = result.Append('_');

                _ = result.Append(char.ToLowerInvariant(c));
            }
            else
                _ = result.Append(c);
        }

        return result.ToString();
    }
}

/// <summary>
///     Represents a single column in a partition specification.
/// </summary>
/// <typeparam name="T">The record type being partitioned.</typeparam>
public sealed class PartitionColumn<T>
{
    /// <summary>
    ///     Gets the name of the property on the record type.
    /// </summary>
    public required string PropertyName { get; init; }

    /// <summary>
    ///     Gets the column name used in the partition path (e.g., "event_date").
    /// </summary>
    public required string ColumnName { get; init; }

    /// <summary>
    ///     Gets the type of the partition value.
    /// </summary>
    public required Type ValueType { get; init; }

    /// <summary>
    ///     Gets a function that extracts the partition value from a record.
    /// </summary>
    public required Func<T?, object?> GetValue { get; init; }
}
