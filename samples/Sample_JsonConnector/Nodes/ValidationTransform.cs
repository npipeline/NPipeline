using System.Text.RegularExpressions;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_JsonConnector;

namespace Sample_JsonConnector.Nodes;

/// <summary>
///     Transform node that validates customer data.
///     Can be configured to filter out invalid records or allow them to pass through.
/// </summary>
public partial class ValidationTransform(bool filterInvalidRecords = false) : TransformNode<Customer, Customer>
{
    private readonly List<string> _validationErrors = [];

    /// <summary>
    ///     Gets validation errors collected during processing.
    /// </summary>
    public IReadOnlyList<string> ValidationErrors => _validationErrors.AsReadOnly();

    [GeneratedRegex(@"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", RegexOptions.Compiled)]
    private static partial Regex EmailRegex();

    /// <summary>
    ///     Validates a customer record and either filters it out or passes it through with validation information.
    /// </summary>
    /// <param name="input">The customer record to validate.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">A cancellation token to observe.</param>
    /// <returns>A task containing validated customer record or throws an exception if invalid and filtering is enabled.</returns>
    public override Task<Customer> ExecuteAsync(
        Customer input,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        var errors = new List<string>();

        // Validate ID
        if (input.Id <= 0)
            errors.Add($"Invalid ID: {input.Id}");

        // Validate name fields
        if (string.IsNullOrWhiteSpace(input.FirstName))
            errors.Add("First name is required");

        if (string.IsNullOrWhiteSpace(input.LastName))
            errors.Add("Last name is required");

        // Validate email
        if (string.IsNullOrWhiteSpace(input.Email))
            errors.Add("Email is required");
        else if (!EmailRegex().IsMatch(input.Email))
            errors.Add($"Invalid email format: {input.Email}");

        // Validate age
        if (input.Age is < 0 or > 150)
            errors.Add($"Invalid age: {input.Age} (must be between 0 and 150)");

        // Validate registration date
        if (input.RegistrationDate == default)
            errors.Add("Registration date is required");
        else if (input.RegistrationDate > DateTime.Now)
            errors.Add($"Registration date cannot be in the future: {input.RegistrationDate:yyyy-MM-dd}");

        // Validate country
        if (string.IsNullOrWhiteSpace(input.Country))
            errors.Add("Country is required");

        // Log validation errors if any
        if (errors.Count > 0)
        {
            var errorMessage = $"Validation failed for Customer ID {input.Id}: {string.Join(", ", errors)}";
            _validationErrors.Add(errorMessage);

            // Log to context if available
            var logger = context.LoggerFactory.CreateLogger("ValidationTransform");

            logger.Log(
                LogLevel.Warning,
                errorMessage);
        }

        // Filter invalid records if configured, otherwise pass through
        if (filterInvalidRecords && errors.Count > 0)
            throw new InvalidOperationException($"Customer validation failed: {string.Join(", ", errors)}");

        // Add validation metadata to the customer
        // We'll use a simple approach by adding a property to indicate validation status
        // In a real-world scenario, you might want to extend the Customer class
        return Task.FromResult(input);
    }

    /// <summary>
    ///     Clears the validation errors.
    /// </summary>
    public void ClearValidationErrors()
    {
        _validationErrors.Clear();
    }
}
