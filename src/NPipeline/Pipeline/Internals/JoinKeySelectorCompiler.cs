using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Attributes.Nodes;
using NPipeline.Graph;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Compiles join key selectors during the builder phase to avoid runtime reflection overhead.
/// </summary>
/// <remarks>
///     This class is responsible for extracting key selector information from join node types
///     and compiling efficient delegates that can extract keys at runtime without further reflection.
/// </remarks>
internal static class JoinKeySelectorCompiler
{
    /// <summary>
    ///     Compiles key selectors for a join node type, extracting them from KeySelectorAttribute declarations.
    /// </summary>
    /// <remarks>
    ///     Returns null delegates if the join node does not have key selectors defined, allowing
    ///     fallback to runtime compilation if needed.
    ///     Throws on configuration errors to provide fail-fast behavior and clear error messages.
    /// </remarks>
    public static (JoinKeySelectorDelegate? Selector1, JoinKeySelectorDelegate? Selector2) Compile(
        Type joinNodeType, Type keyType, Type input1Type, Type input2Type)
    {
        var attributes = joinNodeType.GetCustomAttributes<KeySelectorAttribute>().ToList();
        var selector1Attr = attributes.FirstOrDefault(a => a.TargetType == input1Type);
        var selector2Attr = attributes.FirstOrDefault(a => a.TargetType == input2Type);

        // If attributes are missing, return null and allow BaseJoinNode to compile lazily
        if (selector1Attr is null || selector2Attr is null)
            return (null, null);

        // Compile both selectors. Configuration errors will propagate as InvalidOperationException
        // to enable fail-fast detection during build phase rather than delaying to runtime.
        var compiled1 = CompileKeySelector(input1Type, keyType, selector1Attr.KeyPropertyNames);
        var compiled2 = CompileKeySelector(input2Type, keyType, selector2Attr.KeyPropertyNames);
        return (compiled1, compiled2);
    }

    private static JoinKeySelectorDelegate CompileKeySelector(
        Type inputType, Type keyType, IReadOnlyList<string> propertyNames)
    {
        var itemParameter = Expression.Parameter(typeof(object), "item");
        var typedItem = Expression.Variable(inputType, "typedItem");
        var conversion = Expression.Assign(typedItem, Expression.Convert(itemParameter, inputType));

        var propertyInfos = propertyNames
            .Select(name => inputType.GetProperty(name, BindingFlags.Public | BindingFlags.Instance))
            .ToList();

        var missingPropertyIndex = propertyInfos.FindIndex(p => p is null);

        if (missingPropertyIndex != -1)
        {
            throw new InvalidOperationException(
                $"Could not find a public instance property named '{propertyNames[missingPropertyIndex]}' on type '{inputType.Name}'.");
        }

        Expression keyExpression;

        if (propertyNames.Count == 1)
        {
            var propertyInfo = propertyInfos[0]!;

            if (propertyInfo.PropertyType != keyType)
            {
                throw new InvalidOperationException(
                    $"The property '{propertyInfo.Name}' on type '{inputType.Name}' is of type '{propertyInfo.PropertyType.Name}', but the join key type is '{keyType.Name}'.");
            }

            keyExpression = Expression.Property(typedItem, propertyInfo);
            keyExpression = Expression.Convert(keyExpression, typeof(object));
        }
        else
        {
            // Composite key - verify it's a ValueTuple
            var tupleTypes = keyType.GetGenericArguments();

            // Check if the key type is actually a ValueTuple
            var keyTypeFullName = keyType.FullName;

            if (keyType.IsValueType && keyTypeFullName?.StartsWith("System.ValueTuple", StringComparison.Ordinal) == true &&
                tupleTypes.Length == propertyInfos.Count)
            {
                for (var i = 0; i < propertyInfos.Count; i++)
                {
                    var propertyInfo = propertyInfos[i]!;

                    if (propertyInfo.PropertyType != tupleTypes[i])
                    {
                        throw new InvalidOperationException(
                            $"The property '{propertyInfo.Name}' type '{propertyInfo.PropertyType.Name}' does not match tuple element type '{tupleTypes[i].Name}'.");
                    }
                }

                var propertyAccessors = propertyInfos.Select(p => (Expression)Expression.Property(typedItem, p!)).ToArray();
                var tupleConstructor = keyType.GetConstructor(tupleTypes);

                if (tupleConstructor == null)
                    throw new InvalidOperationException($"Could not find a constructor for the ValueTuple key type '{keyType.Name}'.");

                keyExpression = Expression.New(tupleConstructor, propertyAccessors);
                keyExpression = Expression.Convert(keyExpression, typeof(object));
            }
            else
            {
                throw new InvalidOperationException(
                    $"When using {propertyInfos.Count} properties as composite keys, key type must be a ValueTuple with {propertyInfos.Count} elements, but got '{keyType.Name}'.");
            }
        }

        var blockExpression = Expression.Block(new[] { typedItem }, conversion, keyExpression);
        var lambda = Expression.Lambda<JoinKeySelectorDelegate>(blockExpression, itemParameter);
        return lambda.Compile();
    }
}
