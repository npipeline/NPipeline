using NPipeline.Extensions.Composition;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_Composition;

/// <summary>
///     Sub-pipeline for validating customer data.
/// </summary>
public class ValidationPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the validation pipeline structure.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var validate = builder.AddTransform<CustomerValidator, Customer, ValidatedCustomer>("validate");
        var output = builder.AddSink<PipelineOutputSink<ValidatedCustomer>, ValidatedCustomer>("output");

        builder.Connect(input, validate);
        builder.Connect(validate, output);
    }
}

/// <summary>
///     Sub-pipeline for enriching validated customer data.
/// </summary>
public class EnrichmentPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the enrichment pipeline structure.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<ValidatedCustomer>, ValidatedCustomer>("input");
        var enrich = builder.AddTransform<CustomerEnricher, ValidatedCustomer, EnrichedCustomer>("enrich");
        var output = builder.AddSink<PipelineOutputSink<EnrichedCustomer>, EnrichedCustomer>("output");

        builder.Connect(input, enrich);
        builder.Connect(enrich, output);
    }
}

/// <summary>
///     Transform node for validating customer data.
/// </summary>
public sealed class CustomerValidator : TransformNode<Customer, ValidatedCustomer>
{
    /// <summary>
    ///     Validates customer data and returns a validated customer object.
    /// </summary>
    /// <param name="customer">The customer to validate.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A validated customer with validation results.</returns>
    public override Task<ValidatedCustomer> ExecuteAsync(
        Customer customer,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(customer.Name))
            errors.Add("Name is required");

        if (string.IsNullOrWhiteSpace(customer.Email))
            errors.Add("Email is required");
        else if (!customer.Email.Contains('@'))
            errors.Add("Email must contain @ symbol");

        var validated = new ValidatedCustomer(
            customer,
            errors.Count == 0,
            errors);

        return Task.FromResult(validated);
    }
}

/// <summary>
///     Transform node for enriching validated customer data.
/// </summary>
public sealed class CustomerEnricher : TransformNode<ValidatedCustomer, EnrichedCustomer>
{
    /// <summary>
    ///     Enriches validated customer data with loyalty tier and points.
    /// </summary>
    /// <param name="validatedCustomer">The validated customer to enrich.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>An enriched customer with loyalty information.</returns>
    public override Task<EnrichedCustomer> ExecuteAsync(
        ValidatedCustomer validatedCustomer,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Simulate enrichment logic
        var enriched = new EnrichedCustomer(
            validatedCustomer,
            DateTime.UtcNow,
            DetermineLoyaltyTier(validatedCustomer.OriginalCustomer.Id),
            CalculateLoyaltyPoints(validatedCustomer.OriginalCustomer.Id));

        return Task.FromResult(enriched);
    }

    private static string? DetermineLoyaltyTier(int customerId)
    {
        return customerId switch
        {
            < 100 => "Gold",
            < 500 => "Silver",
            < 1000 => "Bronze",
            _ => null,
        };
    }

    private static int CalculateLoyaltyPoints(int customerId)
    {
        return customerId * 10;
    }
}
