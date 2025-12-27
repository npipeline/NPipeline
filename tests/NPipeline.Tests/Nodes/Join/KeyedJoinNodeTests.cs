using System.Collections.Concurrent;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

public sealed class KeyedJoinNodeTests
{
    [Fact]
    public async Task Runner_WhenKeyedJoinNodeWithCompositeKey_CorrectlyJoinsStreams()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<PayrollRecord>>();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<CompositeKeyedJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<PayrollRecord>>();
        resultStore.Should().HaveCount(3);

        resultStore.Should().BeEquivalentTo([
            new PayrollRecord(1, "A", "Alice", 100m),
            new PayrollRecord(1, "B", "Alice", 200m),
            new PayrollRecord(2, "A", "Bob", 300m),
        ]);
    }


    [Fact]
    public async Task Runner_WhenKeyedJoinNode_CorrectlyJoinsStreams()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<EnrichedUser>>();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<KeyedJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<EnrichedUser>>();
        resultStore.Should().HaveCount(2);

        resultStore.Should().BeEquivalentTo([
            new EnrichedUser(1, "Alice", "Alice's Profile"),
            new EnrichedUser(2, "Bob", "Bob's Profile"),
        ]);
    }

    // Test Data Models
    private sealed record User(int Id, string Name);

    private sealed record UserProfile(int Id, string ProfileInfo);

    private sealed record EnrichedUser(int Id, string Name, string ProfileInfo);

    // Test Node Implementations

    private sealed class UserSource : SourceNode<User>
    {
        public override IDataPipe<User> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var users = new[] { new User(1, "Alice"), new User(2, "Bob") };
            return new StreamingDataPipe<User>(users.ToAsyncEnumerable(), "UserStream");
        }
    }

    private sealed class UserProfileSource : SourceNode<UserProfile>
    {
        public override IDataPipe<UserProfile> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var profiles = new[] { new UserProfile(2, "Bob's Profile"), new UserProfile(1, "Alice's Profile") };
            return new StreamingDataPipe<UserProfile>(profiles.ToAsyncEnumerable(), "ProfileStream");
        }
    }

    [KeySelector(typeof(User), nameof(User.Id))]
    [KeySelector(typeof(UserProfile), nameof(UserProfile.Id))]
    private sealed class UserEnrichmentNode : KeyedJoinNode<int, User, UserProfile, EnrichedUser>
    {
        public override EnrichedUser CreateOutput(User item1, UserProfile item2)
        {
            return new EnrichedUser(item1.Id, item1.Name, item2.ProfileInfo);
        }
    }

    private sealed class EnrichedUserSink(ConcurrentQueue<EnrichedUser> store) : SinkNode<EnrichedUser>
    {
        public override async Task ExecuteAsync(IDataPipe<EnrichedUser> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    // Test Definition

    private sealed class KeyedJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var userSource = builder.AddSource<UserSource, User>("user_source");
            var profileSource = builder.AddSource<UserProfileSource, UserProfile>("profile_source");
            var enrichmentNode = builder.AddJoin<UserEnrichmentNode, User, UserProfile, EnrichedUser>("enrichment_node");
            var sink = builder.AddSink<EnrichedUserSink, EnrichedUser>("sink");

            builder.Connect(userSource, enrichmentNode);
            builder.Connect(profileSource, enrichmentNode);
            builder.Connect(enrichmentNode, sink);
        }
    }

    // Test Data Models for Composite Key
    private sealed record Employee(int EmployeeId, string PayCode, string Name);

    private sealed record PayStub(int EmployeeId, string PayCode, decimal Amount);

    private sealed record PayrollRecord(int EmployeeId, string PayCode, string Name, decimal Amount);

    // Test Node Implementations for Composite Key
    private sealed class EmployeeSource : SourceNode<Employee>
    {
        public override IDataPipe<Employee> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var employees = new[]
            {
                new Employee(1, "A", "Alice"),
                new Employee(1, "B", "Alice"),
                new Employee(2, "A", "Bob"),
            };

            return new StreamingDataPipe<Employee>(employees.ToAsyncEnumerable(), "EmployeeStream");
        }
    }

    private sealed class PayStubSource : SourceNode<PayStub>
    {
        public override IDataPipe<PayStub> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var payStubs = new[]
            {
                new PayStub(1, "B", 200m),
                new PayStub(2, "A", 300m),
                new PayStub(1, "A", 100m),
            };

            return new StreamingDataPipe<PayStub>(payStubs.ToAsyncEnumerable(), "PayStubStream");
        }
    }

    [KeySelector(typeof(Employee), nameof(Employee.EmployeeId), nameof(Employee.PayCode))]
    [KeySelector(typeof(PayStub), nameof(PayStub.EmployeeId), nameof(PayStub.PayCode))]
    private sealed class PayrollJoinNode : KeyedJoinNode<(int, string), Employee, PayStub, PayrollRecord>
    {
        public override PayrollRecord CreateOutput(Employee item1, PayStub item2)
        {
            return new PayrollRecord(item1.EmployeeId, item1.PayCode, item1.Name, item2.Amount);
        }
    }

    private sealed class PayrollRecordSink(ConcurrentQueue<PayrollRecord> store) : SinkNode<PayrollRecord>
    {
        public override async Task ExecuteAsync(IDataPipe<PayrollRecord> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    // Test Definition for Composite Key
    private sealed class CompositeKeyedJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var employeeSource = builder.AddSource<EmployeeSource, Employee>("employee_source");
            var paystubSource = builder.AddSource<PayStubSource, PayStub>("paystub_source");
            var payrollJoinNode = builder.AddJoin<PayrollJoinNode, Employee, PayStub, PayrollRecord>("payroll_join_node");
            var sink = builder.AddSink<PayrollRecordSink, PayrollRecord>("sink");

            builder.Connect(employeeSource, payrollJoinNode);
            builder.Connect(paystubSource, payrollJoinNode);
            builder.Connect(payrollJoinNode, sink);
        }
    }
}
