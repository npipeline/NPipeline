namespace NPipeline.Connectors.PostgreSQL.Mapping
{
    /// <summary>
    /// Specifies that a property should be ignored during convention-based mapping.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class PostgresIgnoreAttribute : Attribute
    {
    }
}
