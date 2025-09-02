using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A transform node that converts between different types with mapping rule support.
/// </summary>
/// <typeparam name="TIn">The input item type to convert from.</typeparam>
/// <typeparam name="TOut">The output item type to convert to.</typeparam>
/// <remarks>
///     <para>
///         TypeConversionNode provides a flexible way to transform items between types, supporting:
///         - Property-to-property mapping with custom converters
///         - Automatic mapping by name matching (case-insensitive)
///         - Record type support with default instantiation
///         - Custom factory functions for complex initialization
///         - Chaining of multiple mapping rules
///     </para>
///     <para>
///         Mapping rules are applied in registration order during execution. Later rules can override
///         mappings established by AutoMap() or earlier Map() calls.
///     </para>
///     <para>
///         Performance considerations:
///         - Expression compilation is cached for efficiency
///         - Reflection is minimized through compiled accessors
///         - Reflection only occurs once during graph construction, not per item
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Simple property mapping
/// var node = new TypeConversionNode&lt;Source, Destination&gt;()
///     .AutoMap()  // Match properties by name
///     .Map(
///         src => src.FirstName,
///         dst => dst.FullName,
///         firstName => $"Mr. {firstName}");
/// 
/// // Custom factory with partial mapping
/// var customNode = new TypeConversionNode&lt;SourceRecord, DestRecord&gt;(
///     factory: _ => new DestRecord { Status = "Active" }
/// )
///     .Map(src => src.Id, dst => dst.Id, id => id)
///     .Map(src => src.Name, dst => dst.DisplayName, name => name.ToUpper());
/// 
/// // Complex transformation
/// var complexNode = new TypeConversionNode&lt;Input, Output&gt;()
///     .Map(
///         input => input.Items,
///         output => output.ItemCount,
///         items => items?.Count ?? 0
///     )
///     .Map(
///         output => output.ProcessedAt,
///         input => DateTimeOffset.UtcNow
///     );
/// </code>
/// </example>
public sealed class TypeConversionNode<TIn, TOut>(Func<TIn, TOut>? factory = null, IExecutionStrategy? executionStrategy = null)
    : TransformNode<TIn, TOut>(executionStrategy)
{
    // Cached type information for record detection (computed once, not on each AutoMap call)
    private static readonly bool IsRecordCached = CacheIsRecordType();

    private static readonly Func<TIn, TOut>? RecordFactoryCached = IsRecordCached
        ? ComputeRecordFactory()
        : null;

    private readonly TypeConverterFactory _converterFactory = TypeConverterFactory.CreateDefault();
    private readonly Func<TIn, TOut> _factory = factory ?? CreateDefaultFactory();
    private readonly List<IRule> _rules = [];
    private bool _sealed;

    /// <summary>
    ///     Map a source property to a destination property using a converter that takes only the source value.
    /// </summary>
    public TypeConversionNode<TIn, TOut> Map<TSrc, TDest>(
        Expression<Func<TIn, TSrc>> source,
        Expression<Func<TOut, TDest>> destination,
        Func<TSrc, TDest> convert)
    {
        EnsureNotSealed();
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(convert);

        var srcGetter = source.Compile();
        var destSetter = AccessorBuilder.CreateSetter(destination);
        _rules.Add(new FromSourcePropertyRule<TSrc, TDest>(srcGetter, (_, value, _, _) => convert(value), destSetter));
        return this;
    }

    /// <summary>
    ///     Map a source property to a destination property using a converter that can inspect the whole input.
    /// </summary>
    public TypeConversionNode<TIn, TOut> Map<TSrc, TDest>(
        Expression<Func<TIn, TSrc>> source,
        Expression<Func<TOut, TDest>> destination,
        Func<TIn, TSrc, TDest> convert)
    {
        EnsureNotSealed();
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(convert);

        var srcGetter = source.Compile();
        var destSetter = AccessorBuilder.CreateSetter(destination);
        _rules.Add(new FromSourcePropertyRule<TSrc, TDest>(srcGetter, (input, value, _, _) => convert(input, value), destSetter));
        return this;
    }

    /// <summary>
    ///     Map a destination property using a converter over the whole input (no single source property).
    /// </summary>
    public TypeConversionNode<TIn, TOut> Map<TDest>(
        Expression<Func<TOut, TDest>> destination,
        Func<TIn, TDest> convert)
    {
        EnsureNotSealed();
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(convert);

        var destSetter = AccessorBuilder.CreateSetter(destination);
        _rules.Add(new FromWholeInputRule<TDest>((input, _, _) => convert(input), destSetter));
        return this;
    }

    /// <summary>
    ///     Automatically maps properties from TIn to TOut by matching names (case-insensitive) and applying registered converters.
    ///     Manual Map() calls can be used after AutoMap() to override specific fields.
    /// </summary>
    public TypeConversionNode<TIn, TOut> AutoMap(StringComparer? comparer = null)
    {
        EnsureNotSealed();

        var nameComparer = comparer ?? StringComparer.OrdinalIgnoreCase;

        var inProps = typeof(TIn).GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var outProps = typeof(TOut).GetProperties(BindingFlags.Instance | BindingFlags.Public);

        Dictionary<string, PropertyInfo> inByName = new(nameComparer);

        foreach (var p in inProps)
        {
            inByName[p.Name] = p;
        }

        foreach (var dest in outProps)
        {
            if (!(dest.CanWrite && dest.SetMethod is not null))
                continue;

            if (!inByName.TryGetValue(dest.Name, out var src))
                continue;

            var srcType = src.PropertyType;
            var destType = dest.PropertyType;

            if (!_converterFactory.TryGetConverter(srcType, destType, out var converter))
                continue;

            var getter = BuildGetterDelegate(src);
            var setter = BuildSetterDelegate(dest, destType);
            AddAutoRuleDynamic(getter, setter, converter, srcType, destType);
        }

        return this;
    }

    private void AddAutoRuleDynamic(Delegate getter, Delegate setter, Delegate converter, Type srcType, Type destType)
    {
        var m = GetType().GetMethod(nameof(AddAutoRule), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(srcType, destType);

        _ = m.Invoke(this, [getter, setter, converter]);
    }

    private void AddAutoRule<TSrc, TDest>(Func<TIn, TSrc> getter, Action<TOut, TDest> setter, Func<TSrc, TDest> converter)
    {
        _rules.Add(new FromSourcePropertyRule<TSrc, TDest>(getter, (_, value, _, _) => converter(value), setter));
    }

    private static Delegate BuildGetterDelegate(PropertyInfo src)
    {
        var param = Expression.Parameter(typeof(TIn), "x");
        var prop = Expression.Property(param, src);
        var lambdaType = typeof(Func<,>).MakeGenericType(typeof(TIn), src.PropertyType);
        return Expression.Lambda(lambdaType, prop, param).Compile();
    }

    private static Delegate BuildSetterDelegate(PropertyInfo dest, Type destType)
    {
        var target = Expression.Parameter(typeof(TOut), "o");
        var value = Expression.Parameter(destType, "v");
        var prop = Expression.Property(target, dest);
        var assign = Expression.Assign(prop, value);
        var lambdaType = typeof(Action<,>).MakeGenericType(typeof(TOut), destType);
        return Expression.Lambda(lambdaType, assign, target, value).Compile();
    }

    public override Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        _sealed = true;
        var output = _factory(item);

        foreach (var rule in _rules)
        {
            cancellationToken.ThrowIfCancellationRequested();
            rule.Apply(item, output, context, cancellationToken);
        }

        return Task.FromResult(output);
    }

    private void EnsureNotSealed()
    {
        if (_sealed)
            throw new InvalidOperationException($"Cannot register mappings after execution has begun for node {GetType().Name}.");
    }

    private static Func<TIn, TOut> CreateDefaultFactory()
    {
        return typeof(TOut) switch
        {
            { IsValueType: true } => _ => Activator.CreateInstance<TOut>(),
            not null when typeof(TOut).GetConstructor(Type.EmptyTypes) is not null =>
                CreateFactoryWithParameterlessConstructor(),
            not null when IsRecordCached => RecordFactoryCached ?? throw new InvalidOperationException("Record factory should not be null"),
            _ => throw new InvalidOperationException(
                $"Type '{typeof(TOut).Name}' does not have a public parameterless constructor. " +
                "Provide a factory to TypeConversionNode or use a type with a parameterless constructor."),
        };

        static Func<TIn, TOut> CreateFactoryWithParameterlessConstructor()
        {
            var parameterlessCtor = typeof(TOut).GetConstructor(Type.EmptyTypes)!;
            var newExpr = Expression.New(parameterlessCtor);
            var lambda = Expression.Lambda<Func<TOut>>(newExpr).Compile();
            return _ => lambda();
        }
    }

    private static bool CacheIsRecordType()
    {
        return IsRecordTypeImpl(typeof(TOut));
    }

    private static bool IsRecordTypeImpl(Type type)
    {
        // Check if type is a record by looking for synthesized methods and attributes
        return type.GetMethod("<Clone>$") is not null ||
               type.GetCustomAttributesData().Any(attr => attr.AttributeType.Name.Contains("RecordAttribute"));
    }

    private static Func<TIn, TOut>? ComputeRecordFactory()
    {
        // For records, we'll create a factory that uses default values for constructor parameters
        var constructors = typeof(TOut).GetConstructors(BindingFlags.Public | BindingFlags.Instance);
        var primaryConstructor = constructors.OrderByDescending(c => c.GetParameters().Length).FirstOrDefault();

        if (primaryConstructor is null)
            throw new InvalidOperationException($"Record type '{typeof(TOut).Name}' has no public constructors.");

        var parameters = primaryConstructor.GetParameters();

        // Create default values for each parameter
        var defaultValues = parameters.Select(p =>
            p.ParameterType.IsValueType
                ? Activator.CreateInstance(p.ParameterType)
                : null
        ).ToArray();

        // Create a simple factory that uses the default values
        return _ =>
        {
            try
            {
                return (TOut)primaryConstructor.Invoke(defaultValues)!;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to create instance of record type '{typeof(TOut).Name}'. " +
                    "Consider providing a custom factory for TypeConversionNode that handles record creation properly.", ex);
            }
        };
    }

    private interface IRule
    {
        void Apply(TIn input, TOut output, PipelineContext context, CancellationToken ct);
    }

    private sealed class FromSourcePropertyRule<TSrc, TDest>(
        Func<TIn, TSrc> getter,
        Func<TIn, TSrc, PipelineContext, CancellationToken, TDest> convert,
        Action<TOut, TDest> setter)
        : IRule
    {
        public void Apply(TIn input, TOut output, PipelineContext context, CancellationToken ct)
        {
            var src = getter(input);
            var dest = convert(input, src, context, ct);
            setter(output, dest);
        }
    }

    private sealed class FromWholeInputRule<TDest>(
        Func<TIn, PipelineContext, CancellationToken, TDest> convert,
        Action<TOut, TDest> setter)
        : IRule
    {
        public void Apply(TIn input, TOut output, PipelineContext context, CancellationToken ct)
        {
            var dest = convert(input, context, ct);
            setter(output, dest);
        }
    }

    /// <summary>
    ///     Minimal accessor builder to compile a destination setter from an expression. Avoids reflection in hot paths.
    /// </summary>
    private static class AccessorBuilder
    {
        public static Action<TOut, TProp> CreateSetter<TProp>(Expression<Func<TOut, TProp>> selector)
        {
            ArgumentNullException.ThrowIfNull(selector);

            var body = RemoveUnary(selector.Body);

            if (body is not MemberExpression memberExpression)
                throw new ArgumentException($"Selector must be a member access expression. Received: {selector}", nameof(selector));

            var targetParam = Expression.Parameter(typeof(TOut), "target");
            var valueParam = Expression.Parameter(typeof(TProp), "value");

            var replaced = (MemberExpression)new ReplaceParameterVisitor(selector.Parameters[0], targetParam).Visit(memberExpression);

            // Validate assignability
            if (replaced.Member is PropertyInfo pi)
            {
                if (!pi.CanWrite || pi.SetMethod is null)
                    throw new ArgumentException($"Member '{GetMemberPath(replaced)}' is a property without a public setter.", nameof(selector));
            }
            else if (replaced.Member is FieldInfo fi)
            {
                if (fi.IsInitOnly)
                    throw new ArgumentException($"Member '{GetMemberPath(replaced)}' is a readonly (init-only) field.", nameof(selector));
            }
            else
                throw new ArgumentException($"Member '{GetMemberPath(replaced)}' is not a property or field.", nameof(selector));

            var assign = Expression.Assign(replaced, valueParam);

            try
            {
                return Expression.Lambda<Action<TOut, TProp>>(assign, targetParam, valueParam).Compile();
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Failed to create setter for '{GetMemberPath(replaced)}'. Details: {ex.Message}", nameof(selector), ex);
            }
        }

        private static Expression RemoveUnary(Expression expr)
        {
            return expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
                ? u.Operand
                : expr;
        }

        private static string GetMemberPath(MemberExpression me)
        {
            var segments = new Stack<string>();
            Expression? current = me;

            while (current is MemberExpression m)
            {
                segments.Push(m.Member.Name);
                current = m.Expression;
            }

            return string.Join('.', segments);
        }

        private sealed class ReplaceParameterVisitor(ParameterExpression from, Expression to) : ExpressionVisitor
        {
            protected override Expression VisitParameter(ParameterExpression node)
            {
                return node == from
                    ? to
                    : base.VisitParameter(node);
            }
        }
    }
}
