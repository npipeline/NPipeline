using Microsoft.AspNetCore.Mvc;
using Sample_HttpPost.Models;
using Sample_HttpPost.Nodes;

namespace Sample_HttpPost.Controllers;

/// <summary>
///     ASP.NET Core controller for receiving webhook HTTP POST requests.
///     This controller accepts webhook data and enqueues it into the WebhookSource channel
///     for processing by the pipeline.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class WebhookController : ControllerBase
{
    private readonly ILogger<WebhookController> _logger;
    private readonly WebhookSource _webhookSource;

    /// <summary>
    ///     Initializes a new instance of the <see cref="WebhookController" /> class.
    /// </summary>
    /// <param name="webhookSource">The singleton webhook source instance.</param>
    /// <param name="logger">The logger instance for logging operations.</param>
    public WebhookController(WebhookSource webhookSource, ILogger<WebhookController> logger)
    {
        ArgumentNullException.ThrowIfNull(webhookSource);
        ArgumentNullException.ThrowIfNull(logger);
        _webhookSource = webhookSource;
        _logger = logger;
    }

    /// <summary>
    ///     Accepts webhook data via HTTP POST and enqueues it for pipeline processing.
    /// </summary>
    /// <param name="webhookData">The webhook data to process.</param>
    /// <param name="cancellationToken">Cancellation token for the request.</param>
    /// <returns>
    ///     HTTP 202 Accepted if the webhook was successfully enqueued.
    ///     HTTP 400 Bad Request if the webhook data is invalid.
    /// </returns>
    [HttpPost]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> PostWebhook(
        [FromBody] WebhookData webhookData,
        CancellationToken cancellationToken)
    {
        if (webhookData == null)
        {
            _logger.LogWarning("Received null webhook data");
            return BadRequest("Webhook data is required");
        }

        try
        {
            _logger.LogInformation(
                "Received webhook POST request for {Id} of type {EventType}",
                webhookData.Id,
                webhookData.EventType
            );

            // Enqueue the webhook data into the channel for pipeline processing
            await _webhookSource.EnqueueAsync(webhookData, cancellationToken);

            _logger.LogInformation(
                "Webhook {Id} successfully enqueued for processing",
                webhookData.Id
            );

            // Return 202 Accepted to indicate the webhook was accepted for processing
            return Accepted(new
            {
                Status = "Accepted",
                Message = "Webhook enqueued for processing",
                WebhookId = webhookData.Id,
                webhookData.EventType,
                _webhookSource.QueuedCount,
            });
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Webhook processing canceled for {Id}", webhookData.Id);
            return StatusCode(StatusCodes.Status499ClientClosedRequest);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing webhook {Id}", webhookData.Id);
            return StatusCode(StatusCodes.Status500InternalServerError, "Internal server error");
        }
    }

    /// <summary>
    ///     Gets the current status of the webhook processing.
    /// </summary>
    /// <returns>
    ///     HTTP 200 OK with status information about the webhook source.
    /// </returns>
    [HttpGet("status")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public IActionResult GetStatus()
    {
        _logger.LogInformation("Webhook status requested");

        return Ok(new
        {
            Status = "Running",
            _webhookSource.QueuedCount,
            _webhookSource.TotalEnqueued,
            Timestamp = DateTime.UtcNow,
        });
    }
}
