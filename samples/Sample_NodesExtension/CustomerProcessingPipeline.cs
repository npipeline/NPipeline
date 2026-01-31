using NPipeline.Extensions.Nodes;
using NPipeline.Pipeline;

namespace Sample_NodesExtension;

/// <summary>
///     Simple data model representing a customer record.
/// </summary>
public class CustomerRecord
{
    public int Id { get; set; }
    public string? Name { get; set; }
    public string? Email { get; set; }
    public string? Phone { get; set; }
    public int Age { get; set; }
    public decimal AccountBalance { get; set; }
    public DateTime CreatedDate { get; set; }
    public List<string> Tags { get; set; } = [];

    public override string ToString()
    {
        return $"ID: {Id}, Name: {Name}, Email: {Email}, Phone: {Phone}, Age: {Age}, " +
               $"Balance: {AccountBalance:C}, Created: {CreatedDate:yyyy-MM-dd}, " +
               $"Tags: [{string.Join(", ", Tags)}]";
    }
}

/// <summary>
///     Pipeline definition demonstrating NPipeline.Extensions.Nodes functionality.
///     This pipeline demonstrates:
///     - String cleansing (trim, case conversion)
///     - Numeric validation (positive balance, valid age)
///     - DateTime cleansing (UTC conversion)
///     - Collection cleansing (remove duplicates)
///     - Enrichment (set defaults, computed values)
///     - Filtering (active accounts only)
/// </summary>
public class CustomerProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Source: Generate sample customer data
        var source = builder.AddSource<CustomerDataSource, CustomerRecord>("customer-source");

        // Cleanse: Normalize string values
        var cleanse = builder.AddStringCleansing<CustomerRecord>(
            n => n
                .Trim(x => x.Name)
                .ToLower(x => x.Email)
                .Trim(x => x.Phone),
            "string-cleansing");

        // DateTime Cleansing: Convert all dates to UTC
        var dateClean = builder.AddDateTimeCleansing<CustomerRecord>(
            n => n.ToUtc(x => x.CreatedDate),
            "datetime-cleansing");

        // Enrichment: Add computed values and defaults
        var enrich = builder.AddEnrichment<CustomerRecord>(
            n => n
                .DefaultIfNull(x => x.Name, "Unknown")
                .DefaultIfEmpty(x => x.Email, "no-email@example.com")
                .Compute(x => x.Phone, c => string.IsNullOrEmpty(c.Phone)
                    ? "N/A"
                    : c.Phone),
            "enrichment");

        // Validation: Check data integrity
        var validate = builder.AddValidationNode<CustomerRecord, CustomerValidator>(
            applyDefaultErrorHandler: true);

        // Filtering: Keep only valid accounts
        var filter = builder.AddFilteringNode<CustomerRecord>(
            n => n
                .Where(c => c.AccountBalance >= 0, _ => "Account balance cannot be negative")
                .Where(c => !string.IsNullOrEmpty(c.Email), _ => "Email is required"),
            "account-filter");

        // Sink: Output processed customers
        var sink = builder.AddSink<CustomerConsoleSink, CustomerRecord>("console-sink");

        // Connect pipeline: source -> cleanse -> date-clean -> enrich -> validate -> filter -> sink
        builder.Connect(source, cleanse);
        builder.Connect(cleanse, dateClean);
        builder.Connect(dateClean, enrich);
        builder.Connect(enrich, validate);
        builder.Connect(validate, filter);
        builder.Connect(filter, sink);
    }

    public static string GetDescription()
    {
        return @"Customer Data Processing Pipeline with Nodes Extension:

This sample demonstrates key features of NPipeline.Extensions.Nodes:

1. STRING CLEANSING:
   - Trim whitespace from Name and Phone
   - Convert Email to lowercase for consistency

2. DATETIME CLEANSING:
   - Convert CreatedDate to UTC timezone

3. ENRICHMENT:
   - Set default name if missing
   - Set default email if empty
   - Normalize phone field

4. VALIDATION:
   - Ensure names are not empty
   - Ensure emails are valid format
   - Ensure age is reasonable (18-120)
   - Ensure account balance is non-negative

5. FILTERING:
   - Remove accounts with negative balances
   - Remove records without email addresses

The pipeline processes sample customer data through these stages,
demonstrating how to clean, validate, and enrich data in a single flow.
";
    }
}
