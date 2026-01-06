namespace NPipeline.Extensions.Composition;

/// <summary>
///     Configuration options for composite node sub-pipeline context.
/// </summary>
public sealed class CompositeContextConfiguration
{
    /// <summary>
    ///     Gets the default configuration with no parent inheritance.
    /// </summary>
    public static CompositeContextConfiguration Default => new();

    /// <summary>
    ///     Gets or sets whether to inherit parameters from parent context.
    /// </summary>
    public bool InheritParentParameters { get; set; }

    /// <summary>
    ///     Gets or sets whether to inherit items from parent context.
    /// </summary>
    public bool InheritParentItems { get; set; }

    /// <summary>
    ///     Gets or sets whether to inherit properties from parent context.
    /// </summary>
    public bool InheritParentProperties { get; set; }

    /// <summary>
    ///     Creates a new configuration with all inheritance options enabled.
    /// </summary>
    public static CompositeContextConfiguration InheritAll => new()
    {
        InheritParentParameters = true,
        InheritParentItems = true,
        InheritParentProperties = true,
    };
}
