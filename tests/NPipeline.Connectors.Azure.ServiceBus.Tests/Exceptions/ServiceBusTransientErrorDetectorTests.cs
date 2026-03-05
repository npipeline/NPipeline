using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus.Exceptions;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Exceptions;

public class ServiceBusTransientErrorDetectorTests
{
    private static readonly ServiceBusTransientErrorDetector Detector =
        ServiceBusTransientErrorDetector.Instance;

    public class IsTransient
    {
        [Fact]
        public void IsTransient_WithNullException_ReturnsFalse()
        {
            Detector.IsTransient(null).Should().BeFalse();
        }

        [Fact]
        public void IsTransient_WithNonServiceBusException_DelegatesToBase()
        {
            var ex = new Exception("generic error");

            // Base class (AzureTransientErrorDetector) handles non-SB exceptions
            // it should not throw
            var result = Detector.IsTransient(ex);
            result.Should().BeFalse();
        }

        [Theory]
        [InlineData(ServiceBusFailureReason.ServiceBusy)]
        [InlineData(ServiceBusFailureReason.ServiceTimeout)]
        [InlineData(ServiceBusFailureReason.ServiceCommunicationProblem)]
        [InlineData(ServiceBusFailureReason.QuotaExceeded)]
        [InlineData(ServiceBusFailureReason.GeneralError)]
        public void IsTransient_WithTransientServiceBusReason_ReturnsTrue(ServiceBusFailureReason reason)
        {
            var ex = new ServiceBusException("Test error", reason);
            Detector.IsTransient(ex).Should().BeTrue();
        }

        [Theory]
        [InlineData(ServiceBusFailureReason.MessageLockLost)]
        [InlineData(ServiceBusFailureReason.SessionLockLost)]
        [InlineData(ServiceBusFailureReason.MessageNotFound)]
        [InlineData(ServiceBusFailureReason.SessionCannotBeLocked)]
        [InlineData(ServiceBusFailureReason.MessagingEntityDisabled)]
        [InlineData(ServiceBusFailureReason.MessagingEntityNotFound)]
        [InlineData(ServiceBusFailureReason.MessageSizeExceeded)]
        [InlineData(ServiceBusFailureReason.MessagingEntityAlreadyExists)]
        public void IsTransient_WithNonTransientServiceBusReason_ReturnsFalse(
            ServiceBusFailureReason reason)
        {
            var ex = new ServiceBusException("Test error", reason);
            Detector.IsTransient(ex).Should().BeFalse();
        }
    }

    public class IsRateLimited
    {
        [Fact]
        public void IsRateLimited_WithNull_ReturnsFalse()
        {
            Detector.IsRateLimited(null).Should().BeFalse();
        }

        [Fact]
        public void IsRateLimited_WithQuotaExceededException_ReturnsTrue()
        {
            var ex = new ServiceBusException("Quota exceeded",
                ServiceBusFailureReason.QuotaExceeded);

            Detector.IsRateLimited(ex).Should().BeTrue();
        }

        [Fact]
        public void IsRateLimited_WithServiceBusyException_ReturnsFalse()
        {
            var ex = new ServiceBusException("Service busy",
                ServiceBusFailureReason.ServiceBusy);

            Detector.IsRateLimited(ex).Should().BeFalse();
        }

        [Fact]
        public void IsRateLimited_WithGenericException_ReturnsFalse()
        {
            var ex = new Exception("Some error");
            Detector.IsRateLimited(ex).Should().BeFalse();
        }
    }

    public class SingletonInstance
    {
        [Fact]
        public void Instance_AlwaysReturnsSameObject()
        {
            var instance1 = ServiceBusTransientErrorDetector.Instance;
            var instance2 = ServiceBusTransientErrorDetector.Instance;
            instance1.Should().BeSameAs(instance2);
        }
    }
}
