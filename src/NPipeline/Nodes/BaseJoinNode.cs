using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Attributes.Nodes;
using NPipeline.Pipeline;
using NPipeline.Pipeline.Internals;

namespace NPipeline.Nodes;

/// <summary>
///     Base class for join nodes that provides common functionality for processing two input streams.
/// </summary>
/// <typeparam name="TKey">The type of the key used for joining. Must be not-null.</typeparam>
/// <typeparam name="TIn1">The type of the data from the first input stream.</typeparam>
/// <typeparam name="TIn2">The type of the data from the second input stream.</typeparam>
/// <typeparam name="TOut">The type of the output data after the join.</typeparam>
public abstract class BaseJoinNode<TKey, TIn1, TIn2, TOut> : IJoinNode where TKey : notnull
{
    private static readonly Lazy<FallbackProjection<TIn1>> _leftFallbackProjection = new(() =>
        BuildFallbackProjection<TIn1>("left", nameof(CreateOutputFromLeft)), LazyThreadSafetyMode.ExecutionAndPublication);

    private static readonly Lazy<FallbackProjection<TIn2>> _rightFallbackProjection = new(() =>
        BuildFallbackProjection<TIn2>("right", nameof(CreateOutputFromRight)), LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly Lazy<(Func<TIn1, TKey> getKey1, Func<TIn2, TKey> getKey2)> _keySelectors;

    /// <summary>
    ///     Initializes a new instance of the <see cref="BaseJoinNode{TKey, TIn1, TIn2, TOut}" /> class.
    /// </summary>
    protected BaseJoinNode()
    {
        // Build lazily to ensure derived class attributes are available; still only once per node instance.
        _keySelectors = new Lazy<(Func<TIn1, TKey>, Func<TIn2, TKey>)>(GetKeySelectorsInternal, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>
    ///     Processes the combined input stream and returns a joined output stream as object?.
    /// </summary>
    public ValueTask<IAsyncEnumerable<object?>> ExecuteAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        var outputStream = ExecuteJoinAsync(inputStream, context, cancellationToken);

        return ValueTask.FromResult(ToObjectStream(outputStream, cancellationToken));

        async IAsyncEnumerable<object?> ToObjectStream(IAsyncEnumerable<TOut> source, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var item in source.WithCancellation(ct))
            {
                yield return item;
            }
        }
    }

    /// <summary>
    ///     Asynchronously disposes of the node. This can be overridden by derived classes to release resources.
    /// </summary>
    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     When implemented in a derived class, creates the output item from the two joined input items.
    /// </summary>
    public abstract TOut CreateOutput(TIn1 item1, TIn2 item2);

    /// <summary>
    ///     When implemented in a derived class, creates an output item from a left item when there's no right match.
    ///     Used for left outer and full outer joins.
    /// </summary>
    public virtual TOut CreateOutputFromLeft(TIn1 item1)
    {
        var fallback = _leftFallbackProjection.Value;

        if (fallback.Projection is null)
            throw new NotSupportedException(fallback.FailureMessage);

        return fallback.Projection(item1);
    }

    /// <summary>
    ///     When implemented in a derived class, creates an output item from a right item when there's no left match.
    ///     Used for right outer and full outer joins.
    /// </summary>
    public virtual TOut CreateOutputFromRight(TIn2 item2)
    {
        var fallback = _rightFallbackProjection.Value;

        if (fallback.Projection is null)
            throw new NotSupportedException(fallback.FailureMessage);

        return fallback.Projection(item2);
    }

    /// <summary>
    ///     When implemented in a derived class, performs the join operation on the input stream.
    /// </summary>
    protected abstract IAsyncEnumerable<TOut> ExecuteJoinAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Gets the key selectors for both input types.
    /// </summary>
    protected (Func<TIn1, TKey> GetKey1, Func<TIn2, TKey> GetKey2) GetKeySelectors()
    {
        return _keySelectors.Value;
    }

    private (Func<TIn1, TKey>, Func<TIn2, TKey>) GetKeySelectorsInternal()
    {
        // Check if pre-compiled selectors are available from the builder phase
        if (JoinKeySelectorRegistry.TryGetSelectors(GetType(), out var rawSelector1, out var rawSelector2))
        {
            if (rawSelector1 is not null && rawSelector2 is not null)
            {
                // Convert the untyped delegates back to typed selectors
                // Use explicit non-null assertions since we've already checked for null
                var sel1 = rawSelector1;
                var sel2 = rawSelector2;
                return (item => (TKey)sel1(item!)!, item => (TKey)sel2(item!)!);
            }
        }

        // Fall back to runtime compilation if pre-compiled selectors are not available
        var attributes = GetType().GetCustomAttributes<KeySelectorAttribute>().ToList();
        var selector1Attr = attributes.FirstOrDefault(a => a.TargetType == typeof(TIn1));
        var selector2Attr = attributes.FirstOrDefault(a => a.TargetType == typeof(TIn2));

        if (selector1Attr is null || selector2Attr is null)
            throw new InvalidOperationException($"Join node requires two {nameof(KeySelectorAttribute)} declarations, one for each input type.");

        return (
            CreateKeySelector<TIn1>(selector1Attr.KeyPropertyNames),
            CreateKeySelector<TIn2>(selector2Attr.KeyPropertyNames)
        );
    }

    private Func<T, TKey> CreateKeySelector<T>(IReadOnlyList<string> propertyNames)
    {
        var itemParameter = Expression.Parameter(typeof(T), "item");

        var propertyInfos = propertyNames
            .Select(name => typeof(T).GetProperty(name, BindingFlags.Public | BindingFlags.Instance))
            .ToList();

        var missingPropertyIndex = propertyInfos.FindIndex(p => p is null);

        if (missingPropertyIndex != -1)
        {
            throw new InvalidOperationException(
                $"Could not find a public instance property named '{propertyNames[missingPropertyIndex]}' on type '{typeof(T).Name}'.");
        }

        if (propertyNames.Count == 1)
        {
            var propertyInfo = propertyInfos[0]!;

            if (propertyInfo.PropertyType != typeof(TKey))
            {
                throw new InvalidOperationException(
                    $"The property '{propertyInfo.Name}' on type '{typeof(T).Name}' is of type '{propertyInfo.PropertyType.Name}', but the join key type TKey is '{typeof(TKey).Name}'.");
            }

            var propertyAccess = Expression.Property(itemParameter, propertyInfo);
            var castToKeyType = Expression.Convert(propertyAccess, typeof(TKey));
            return Expression.Lambda<Func<T, TKey>>(castToKeyType, itemParameter).Compile();
        }

        var tupleTypes = typeof(TKey).GetGenericArguments();

        if (!typeof(TKey).FullName!.StartsWith("System.ValueTuple", StringComparison.Ordinal) || tupleTypes.Length != propertyInfos.Count)
        {
            throw new InvalidOperationException(
                $"When using composite keys, TKey must be a ValueTuple with a number of elements matching the key properties. Expected {propertyInfos.Count} elements for TKey '{typeof(TKey).Name}'.");
        }

        for (var i = 0; i < propertyInfos.Count; i++)
        {
            var propertyInfo = propertyInfos[i]!;

            if (propertyInfo.PropertyType != tupleTypes[i])
            {
                throw new InvalidOperationException(
                    $"The property '{propertyInfo.Name}' on type '{typeof(T).Name}' has type '{propertyInfo.PropertyType.Name}', which does not match the corresponding tuple element type '{tupleTypes[i].Name}' in TKey '{typeof(TKey).Name}'.");
            }
        }

        var propertyAccessors = propertyInfos.Select(p => Expression.Property(itemParameter, p!)).ToArray<Expression>();
        var tupleConstructor = typeof(TKey).GetConstructor(tupleTypes);

        if (tupleConstructor is null)
            throw new InvalidOperationException($"Could not find a constructor for the ValueTuple TKey '{typeof(TKey).Name}'.");

        var newTupleExpression = Expression.New(tupleConstructor, propertyAccessors);
        return Expression.Lambda<Func<T, TKey>>(newTupleExpression, itemParameter).Compile();
    }

    private static FallbackProjection<TSource> BuildFallbackProjection<TSource>(string sideName, string methodName)
    {
        var (projection, failureReason) = TryCreateProjection<TSource>();

        var failureMessage = failureReason ??
                             $"No automatic projection from {typeof(TSource).Name} to {typeof(TOut).Name} could be generated for {sideName}-only outputs. Override {methodName} to provide custom behaviour.";

        return new FallbackProjection<TSource>(projection, failureMessage);
    }

    private static (Func<TSource, TOut>? Projection, string? FailureReason) TryCreateProjection<TSource>()
    {
        if (typeof(TOut).IsAssignableFrom(typeof(TSource)))
            return (static source => (TOut)(object)source!, null);

        var sourceType = typeof(TSource);
        var sourceMembers = BuildSourceMemberMap(sourceType);

        var bestScore = -1;
        Func<TSource, TOut>? bestFactory = null;

        foreach (var ctor in typeof(TOut).GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
        {
            var parameters = ctor.GetParameters();
            var sourceParameter = Expression.Parameter(sourceType, "source");
            var argumentExpressions = new Expression[parameters.Length];
            var matchedCount = 0;
            var failed = false;

            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];

                if (parameter.ParameterType.IsAssignableFrom(sourceType))
                {
                    argumentExpressions[i] = sourceParameter;
                    matchedCount++;
                    continue;
                }

                if (TryGetSourceValue<TSource>(sourceMembers, parameter, sourceParameter, out var valueExpression))
                {
                    argumentExpressions[i] = valueExpression;
                    matchedCount++;
                    continue;
                }

                if (parameter.HasDefaultValue)
                {
                    argumentExpressions[i] = Expression.Constant(parameter.DefaultValue, parameter.ParameterType);
                    continue;
                }

                if (!parameter.ParameterType.IsValueType || Nullable.GetUnderlyingType(parameter.ParameterType) is not null)
                {
                    argumentExpressions[i] = Expression.Constant(null, parameter.ParameterType);
                    continue;
                }

                // No data available for a required value type parameter – unable to build projection via this constructor
                failed = true;
                break;
            }

            if (failed || matchedCount == 0)
                continue;

            var body = Expression.New(ctor, argumentExpressions);
            var lambda = Expression.Lambda<Func<TSource, TOut>>(body, sourceParameter).Compile();

            if (matchedCount > bestScore)
            {
                bestScore = matchedCount;
                bestFactory = lambda;
            }
        }

        if (bestFactory is not null)
            return (bestFactory, null);

        if (TryCreatePropertyInitializer<TSource>(sourceMembers, out var propertyFactory))
            return (propertyFactory, null);

        return (null, $"Unable to infer how to project {typeof(TSource).Name} into {typeof(TOut).Name}.");
    }

    private static bool TryGetSourceValue<TSource>(IReadOnlyDictionary<string, SourceMember> sourceMembers, ParameterInfo parameter,
        ParameterExpression sourceParameter, out Expression valueExpression)
    {
        if (!string.IsNullOrEmpty(parameter.Name) &&
            sourceMembers.TryGetValue(parameter.Name, out var member) &&
            TryConvert(member.CreateAccess(sourceParameter), parameter.ParameterType, out valueExpression))
            return true;

        // No direct name match; fall back to type-only match if a single candidate exists
        valueExpression = default!;
        return false;
    }

    private static bool TryCreatePropertyInitializer<TSource>(IReadOnlyDictionary<string, SourceMember> sourceMembers,
        out Func<TSource, TOut>? projection)
    {
        projection = null;
        var parameterlessCtor = typeof(TOut).GetConstructor(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance, null, Type.EmptyTypes, null);

        if (parameterlessCtor is null)
            return false;

        var writableProperties = typeof(TOut).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && p.SetMethod is { IsStatic: false, IsPublic: true })
            .ToList();

        if (writableProperties.Count == 0)
            return false;

        var sourceParameter = Expression.Parameter(typeof(TSource), "source");
        var targetVariable = Expression.Variable(typeof(TOut), "target");
        var assignments = new List<Expression> { Expression.Assign(targetVariable, Expression.New(parameterlessCtor)) };
        var assignedCount = 0;

        foreach (var property in writableProperties)
        {
            if (sourceMembers.TryGetValue(property.Name, out var member) &&
                TryConvert(member.CreateAccess(sourceParameter), property.PropertyType, out var valueExpression))
            {
                assignments.Add(Expression.Assign(Expression.Property(targetVariable, property), valueExpression));
                assignedCount++;
            }
        }

        if (assignedCount == 0)
            return false;

        assignments.Add(targetVariable);
        var body = Expression.Block(new[] { targetVariable }, assignments);
        projection = Expression.Lambda<Func<TSource, TOut>>(body, sourceParameter).Compile();
        return true;
    }

    private static IReadOnlyDictionary<string, SourceMember> BuildSourceMemberMap(Type sourceType)
    {
        var map = new Dictionary<string, SourceMember>(StringComparer.OrdinalIgnoreCase);

        foreach (var property in sourceType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
        {
            if (!property.CanRead || property.GetMethod is not { IsStatic: false } getter)
                continue;

            if (getter.IsPublic)
                map[property.Name] = new SourceMember(property);
            else
            {
                var backingField = sourceType.GetField($"<{property.Name}>k__BackingField",
                    BindingFlags.NonPublic | BindingFlags.Instance);

                if (backingField is not null)
                    map[property.Name] = new SourceMember(backingField);
            }
        }

        foreach (var field in sourceType.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
        {
            map.TryAdd(field.Name, new SourceMember(field));
        }

        return map;
    }

    private static bool TryConvert(Expression valueExpression, Type targetType, out Expression converted)
    {
        if (targetType.IsAssignableFrom(valueExpression.Type))
        {
            converted = valueExpression;
            return true;
        }

        try
        {
            converted = Expression.Convert(valueExpression, targetType);
            return true;
        }
        catch (InvalidOperationException)
        {
            converted = default!;
            return false;
        }
    }

    private sealed record FallbackProjection<TSource>(Func<TSource, TOut>? Projection, string FailureMessage);

    private sealed class SourceMember
    {
        private readonly FieldInfo? _field;
        private readonly PropertyInfo? _property;

        public SourceMember(PropertyInfo property)
        {
            _property = property;
            MemberType = property.PropertyType;
        }

        public SourceMember(FieldInfo field)
        {
            _field = field;
            MemberType = field.FieldType;
        }

        public Type MemberType { get; }

        public Expression CreateAccess(ParameterExpression sourceParameter)
        {
            if (_property is not null)
                return Expression.Property(sourceParameter, _property);

            return Expression.Field(sourceParameter, _field!);
        }
    }
}
