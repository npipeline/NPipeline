namespace NPipeline.Connectors.Aws.Redshift.Mapping;

/// <summary>Naming convention for mapping .NET property names to Redshift column names.</summary>
public enum RedshiftNamingConvention
{
    /// <summary>Convert PascalCase/camelCase property names to lower_snake_case (default).</summary>
    PascalToSnakeCase,

    /// <summary>Use the property name as-is (relies on double-quoting in SQL).</summary>
    AsIs,

    /// <summary>Convert all characters to lowercase.</summary>
    Lowercase,
}
