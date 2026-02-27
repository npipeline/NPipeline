using NPipeline.Connectors.RabbitMQ.Configuration;

namespace NPipeline.Connectors.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqSourceOptionsTests
{
    [Fact]
    public void Validate_Succeeds_With_Valid_QueueName()
    {
        var options = new RabbitMqSourceOptions { QueueName = "test-queue" };
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Throws_When_QueueName_Is_Empty()
    {
        var options = new RabbitMqSourceOptions { QueueName = "" };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*QueueName*");
    }

    [Fact]
    public void Validate_Throws_When_QueueName_Is_Whitespace()
    {
        var options = new RabbitMqSourceOptions { QueueName = "   " };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*QueueName*");
    }

    [Fact]
    public void Default_Values_Are_Correct()
    {
        var options = new RabbitMqSourceOptions { QueueName = "q" };

        options.PrefetchCount.Should().Be(100);
        options.PrefetchGlobal.Should().BeFalse();
        options.RequeueOnNack.Should().BeTrue();
        options.Exclusive.Should().BeFalse();
        options.ConsumerDispatchConcurrency.Should().Be(1);
        options.InternalBufferCapacity.Should().Be(1000);
        options.MaxRetries.Should().Be(3);
        options.RetryBaseDelayMs.Should().Be(100);
        options.ContinueOnDeserializationError.Should().BeFalse();
        options.MaxDeliveryAttempts.Should().Be(5);
        options.RejectOnMaxDeliveryAttempts.Should().BeTrue();
    }

    [Fact]
    public void Validate_Throws_When_InternalBufferCapacity_Is_Zero()
    {
        var options = new RabbitMqSourceOptions { QueueName = "q", InternalBufferCapacity = 0 };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*InternalBufferCapacity*");
    }

    [Fact]
    public void Validate_Throws_When_MaxDeliveryAttempts_Is_Zero()
    {
        var options = new RabbitMqSourceOptions { QueueName = "q", MaxDeliveryAttempts = 0 };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*MaxDeliveryAttempts*");
    }

    [Fact]
    public void Validate_Succeeds_When_MaxDeliveryAttempts_Is_Null()
    {
        var options = new RabbitMqSourceOptions { QueueName = "q", MaxDeliveryAttempts = null };
        var act = () => options.Validate();
        act.Should().NotThrow();
    }
}
