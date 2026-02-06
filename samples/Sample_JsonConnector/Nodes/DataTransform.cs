using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_JsonConnector;

namespace Sample_JsonConnector.Nodes;

/// <summary>
///     Transform node that enriches and transforms customer data.
/// </summary>
public class DataTransform : TransformNode<Customer, Customer>
{
    private readonly bool _addCalculatedFields;
    private readonly bool _normalizeData;

    /// <summary>
    ///     Initializes a new instance of DataTransform class.
    /// </summary>
    /// <param name="addCalculatedFields">Whether to add calculated fields to the customer data.</param>
    /// <param name="normalizeData">Whether to normalize data (e.g., country names).</param>
    public DataTransform(bool addCalculatedFields = true, bool normalizeData = true)
    {
        _addCalculatedFields = addCalculatedFields;
        _normalizeData = normalizeData;
    }

    /// <summary>
    ///     Transforms customer data by adding calculated fields and normalizing values.
    /// </summary>
    /// <param name="input">The customer record to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">A cancellation token to observe.</param>
    /// <returns>A task containing the transformed customer record.</returns>
    public override Task<Customer> ExecuteAsync(
        Customer input,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        // Create a copy of the customer to avoid modifying the original
        var transformedCustomer = new Customer
        {
            Id = input.Id,
            FirstName = NormalizeName(input.FirstName),
            LastName = NormalizeName(input.LastName),
            Email = input.Email.ToLowerInvariant().Trim(),
            Age = input.Age,
            RegistrationDate = input.RegistrationDate,
            Country = _normalizeData
                ? input.NormalizedCountry
                : input.Country,
            IsActive = input.IsActive,
        };

        // Log the transformation
        var logger = context.LoggerFactory.CreateLogger("DataTransform");

        logger.Log(
            LogLevel.Debug,
            "Transformed customer {CustomerId}: {OriginalName} -> {TransformedName}",
            transformedCustomer.Id,
            $"{input.FirstName} {input.LastName}",
            $"{transformedCustomer.FirstName} {transformedCustomer.LastName}");

        return Task.FromResult(transformedCustomer);
    }

    /// <summary>
    ///     Normalizes a name by trimming whitespace and capitalizing the first letter.
    /// </summary>
    /// <param name="name">The name to normalize.</param>
    /// <returns>The normalized name.</returns>
    private static string NormalizeName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return string.Empty;

        return name.Trim().ToLowerInvariant() switch
        {
            var trimmed when string.IsNullOrEmpty(trimmed) => string.Empty,
            var normalized when normalized.Length == 1 => normalized.ToUpperInvariant(),
            var normalized => char.ToUpperInvariant(normalized[0]) + normalized[1..],
        };
    }
}
