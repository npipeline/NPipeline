using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.Exceptions;

namespace NPipeline.Connectors.Azure.ServiceBus.Exceptions;

/// <summary>
///     Determines whether an Azure Service Bus exception represents a transient (retryable) error.
///     Extends <see cref="AzureTransientErrorDetector" /> with Service Bus-specific failure reasons.
/// </summary>
public sealed class ServiceBusTransientErrorDetector : AzureTransientErrorDetector
{
    /// <summary>A shared singleton instance.</summary>
    public static readonly ServiceBusTransientErrorDetector Instance = new();

    /// <inheritdoc />
    public override bool IsTransient(Exception? exception)
    {
        if (exception is null)
            return false;

        if (exception is ServiceBusException sbe)
            return IsTransientServiceBusReason(sbe);

        return base.IsTransient(exception);
    }

    /// <inheritdoc />
    public override bool IsRateLimited(Exception? exception)
    {
        return exception is ServiceBusException { Reason: ServiceBusFailureReason.QuotaExceeded }
               || base.IsRateLimited(exception);
    }

    private static bool IsTransientServiceBusReason(ServiceBusException ex)
    {
        return ex.Reason switch
        {
            ServiceBusFailureReason.ServiceBusy => true,
            ServiceBusFailureReason.ServiceTimeout => true,
            ServiceBusFailureReason.ServiceCommunicationProblem => true,
            ServiceBusFailureReason.QuotaExceeded => true,
            ServiceBusFailureReason.GeneralError => true,
            ServiceBusFailureReason.MessageLockLost => false,
            ServiceBusFailureReason.SessionLockLost => false,
            ServiceBusFailureReason.MessageNotFound => false,
            ServiceBusFailureReason.SessionCannotBeLocked => false,
            ServiceBusFailureReason.MessagingEntityDisabled => false,
            ServiceBusFailureReason.MessagingEntityNotFound => false,
            ServiceBusFailureReason.MessageSizeExceeded => false,
            ServiceBusFailureReason.MessagingEntityAlreadyExists => false,
            _ => ex.IsTransient, // Delegate to SDK's own judgment for unknown reasons
        };
    }
}
