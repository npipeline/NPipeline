using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_HttpPost.Models;

namespace Sample_HttpPost.Nodes;

/// <summary>
///     Transform node that validates webhook data according to business rules.
///     This node ensures that required fields are present and valid before further processing.
/// </summary>
public class ValidationTransform : TransformNode<WebhookData, ValidatedWebhookData>
{
    private readonly ILogger<ValidationTransform> _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ValidationTransform" /> class.
    /// </summary>
    /// <param name="logger">The logger instance for logging operations.</param>
    public ValidationTransform(ILogger<ValidationTransform> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <summary>
    ///     Validates the input webhook data according to business rules.
    /// </summary>
    /// <param name="item">The input webhook data to validate.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A task containing the validated webhook data.</returns>
    /// <exception cref="ArgumentException">Thrown when validation fails.</exception>
    public override async Task<ValidatedWebhookData> ExecuteAsync(
        WebhookData item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Validating webhook {Id} of type {EventType}",
            item.Id,
            item.EventType
        );

        var validationErrors = new List<string>();

        // Validate Id
        if (string.IsNullOrWhiteSpace(item.Id))
            validationErrors.Add("Webhook ID is required");

        // Validate EventType
        if (string.IsNullOrWhiteSpace(item.EventType))
            validationErrors.Add("EventType is required");
        else if (item.EventType.Length > 100)
            validationErrors.Add("EventType must not exceed 100 characters");

        // Validate Payload
        if (item.Payload == null)
            validationErrors.Add("Payload is required");
        else if (item.Payload.Count == 0)
            validationErrors.Add("Payload must contain at least one item");

        // Validate Timestamp
        if (item.Timestamp == default)
            validationErrors.Add("Timestamp is required");
        else if (item.Timestamp > DateTime.UtcNow.AddMinutes(5))
            validationErrors.Add("Timestamp cannot be more than 5 minutes in the future");

        if (validationErrors.Count > 0)
        {
            var errorMessage = $"Validation failed for webhook {item.Id}: {string.Join(", ", validationErrors)}";

            _logger.LogWarning(
                "Validation failed for webhook {Id}. Errors: {Errors}",
                item.Id,
                string.Join(", ", validationErrors)
            );

            throw new ArgumentException(errorMessage, nameof(item));
        }

        var validatedData = new ValidatedWebhookData(
            item.Id,
            item.EventType,
            item.Payload!,
            item.Timestamp,
            DateTime.UtcNow
        );

        _logger.LogInformation(
            "Validation passed for webhook {Id} of type {EventType}",
            validatedData.Id,
            validatedData.EventType
        );

        return await Task.FromResult(validatedData);
    }
}
