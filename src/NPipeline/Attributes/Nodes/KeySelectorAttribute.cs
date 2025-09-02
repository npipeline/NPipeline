namespace NPipeline.Attributes.Nodes;

/// <summary>
///     Specifies the property to be used as a key for a given type in a KeyedJoin merge operation.
///     Apply this attribute multiple times to a node class that uses the KeyedJoin strategy—once for each input type.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class KeySelectorAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="KeySelectorAttribute" /> class.
    /// </summary>
    /// <param name="targetType">The type of the data for which this key selector applies.</param>
    /// <param name="keyPropertyNames">The names of the properties on the target type to be used as the composite join key.</param>
    public KeySelectorAttribute(Type targetType, params string[] keyPropertyNames)
    {
        TargetType = targetType ?? throw new ArgumentNullException(nameof(targetType));

        if (keyPropertyNames is null || keyPropertyNames.Length == 0)
            throw new ArgumentException("At least one key property name must be provided.", nameof(keyPropertyNames));

        KeyPropertyNames = keyPropertyNames;
    }

    /// <summary>
    ///     Gets the type of the data for which this key selector applies.
    /// </summary>
    public Type TargetType { get; }

    /// <summary>
    ///     Gets the name of the property to be used as the join key.
    /// </summary>
    public IReadOnlyList<string> KeyPropertyNames { get; }
}
