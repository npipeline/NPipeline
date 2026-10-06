using Xunit;
using System.Data;
using AwesomeAssertions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Sql;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Tests.Sql;

public sealed class SqlSourceNodeTests
{
    private readonly FakeDatabase _database = new();

    private TestSource<T> Source<T>(Func<TestReadOptions, TestReadOptions>? configure = null, Func<SqlRow, T>? map = null) =>
        new((configure ?? (o => o))(new TestReadOptions { Database = _database, Query = "SELECT * FROM orders" }), map);

    private void Rows(params (string Name, Type Type)[] columns) => _database.QueryResult = Table(columns);

    private void Add(params object?[] values) => _database.QueryResult.Rows.Add(values.Select(v => v ?? DBNull.Value).ToArray());

    private static DataTable Table((string Name, Type Type)[] columns)
    {
        var table = new DataTable();

        foreach (var (name, type) in columns)
        {
            table.Columns.Add(name, type);
        }

        return table;
    }

    [Fact]
    public async Task Maps_columns_to_members_by_name_ignoring_case_and_order()
    {
        Rows(("status", typeof(int)), ("NOTE", typeof(string)), ("id", typeof(int)));
        Add(1, "a", 1);
        Add(0, null, 2);

        var rows = await SqlNodeRunner.ReadAsync(Source<Order>());

        rows.Should().Equal(new Order { Id = 1, Status = Status.Active, Note = "a" }, new Order { Id = 2, Status = Status.Pending });
    }

    [Fact]
    public async Task Converts_column_types_to_member_types()
    {
        // A bigint count into an int, text into a decimal (invariantly), and a date into DateOnly.
        Rows(("Id", typeof(long)), ("Amount", typeof(string)), ("Day", typeof(DateTime)));
        Add(7L, "1.5", new DateTime(2026, 1, 2));

        var row = (await SqlNodeRunner.ReadAsync(Source<Converted>())).Single();

        row.Should().Be(new Converted { Id = 7, Amount = 1.5m, Day = new DateOnly(2026, 1, 2) });
    }

    [Fact]
    public async Task Builds_positional_records_through_their_constructor()
    {
        Rows(("Name", typeof(string)), ("Id", typeof(int)));
        Add("Ada", 1);

        (await SqlNodeRunner.ReadAsync(Source<Positional>())).Should().Equal(new Positional(1, "Ada"));
    }

    [Fact]
    public async Task A_row_that_fails_to_map_fails_the_read_with_its_values()
    {
        Rows(("Id", typeof(int)), ("Score", typeof(string)));
        Add(1, "5");
        Add(2, "abc");

        var read = () => SqlNodeRunner.ReadAsync(Source<Scored>());

        var error = (await read.Should().ThrowAsync<RecordMappingException>()).Which;
        error.RecordNumber.Should().Be(2);
        error.Field.Should().Be("Score");
        error.RawExcerpt.Should().Be("Id=2, Score=abc");
    }

    [Fact]
    public async Task A_row_error_handler_can_skip_a_row()
    {
        Rows(("Id", typeof(int)), ("Score", typeof(string)));
        Add(1, "abc");
        Add(2, "5");

        var rows = await SqlNodeRunner.ReadAsync(Source<Scored>(o => o with { RowErrorHandler = _ => RowErrorAction.Skip }));

        rows.Should().Equal(new Scored { Id = 2, Score = 5 });
    }

    [Fact]
    public async Task A_null_for_a_non_nullable_member_is_a_row_error()
    {
        Rows(("Id", typeof(int)), ("Score", typeof(int)));
        Add(1, null);

        var read = () => SqlNodeRunner.ReadAsync(Source<Scored>());

        (await read.Should().ThrowAsync<RecordMappingException>()).Which.Field.Should().Be("Score");
    }

    [Fact]
    public async Task A_missing_required_member_fails_before_the_first_row()
    {
        Rows(("Name", typeof(string)));
        Add("Ada");

        var read = () => SqlNodeRunner.ReadAsync(Source<Required>());

        (await read.Should().ThrowAsync<RecordBindingException>()).Which.MissingColumns.Should().Equal("Id");
    }

    [Fact]
    public async Task A_manual_mapper_reads_the_row()
    {
        Rows(("id", typeof(int)), ("note", typeof(string)), ("score", typeof(string)));
        Add(1, null, "7");

        var rows = await SqlNodeRunner.ReadAsync(Source(map: row => new
        {
            Id = row.Get<int>("ID"),
            Missing = row.IsNull("note"),
            Score = row.Get<int>(2),
            Fallback = row.GetOrDefault("nope", -1),
            Found = row.TryGet<decimal>("score", out var score) ? score : 0m,
            Raw = row["score"],
            row.RecordNumber,
        }));

        rows.Single().Should().BeEquivalentTo(new { Id = 1, Missing = true, Score = 7, Fallback = -1, Found = 7m, Raw = (object)"7", RecordNumber = 1L });
    }

    [Fact]
    public async Task A_manual_mapper_reports_a_value_that_does_not_convert_by_column()
    {
        Rows(("id", typeof(string)));
        Add("x");

        var read = () => SqlNodeRunner.ReadAsync(Source(map: row => row.Get<int>("id")));

        (await read.Should().ThrowAsync<RecordMappingException>()).Which.Field.Should().Be("id");
    }

    [Fact]
    public async Task Query_parameters_are_bound_by_name()
    {
        Rows(("Id", typeof(int)));

        _ = await SqlNodeRunner.ReadAsync(Source<Scored>(o => o with { Parameters = [new DatabaseParameter("@since", 5)] }));

        var query = _database.Executed.Single();
        query.Names.Should().Equal("@since");
        query.Values.Should().Equal(5);
    }

    [Fact]
    public async Task An_offset_checkpoint_resumes_after_the_rows_already_read()
    {
        Rows(("Id", typeof(int)), ("Score", typeof(int)));
        Add(1, 1);
        Add(2, 2);
        var source = Source<Scored>(o => o with { CheckpointStrategy = CheckpointStrategy.InMemory });

        (await SqlNodeRunner.ReadAsync(source)).Should().HaveCount(2);

        Add(3, 3);
        (await SqlNodeRunner.ReadAsync(source)).Select(r => r.Id).Should().Equal(3);
    }

    [Fact]
    public void Options_are_validated()
    {
        var noQuery = () => Source<Scored>(o => o with { Query = " " });
        var cdc = () => Source<Scored>(o => o with { CheckpointStrategy = CheckpointStrategy.CDC });
        var offsetWithoutStorage = () => Source<Scored>(o => o with { CheckpointStrategy = CheckpointStrategy.Offset });

        noQuery.Should().Throw<ArgumentException>();
        cdc.Should().Throw<NotSupportedException>();
        offsetWithoutStorage.Should().Throw<ArgumentException>().WithMessage("*CheckpointStorage*");
    }

    [Fact]
    public async Task Applies_the_naming_policy()
    {
        Rows(("order_id", typeof(int)));
        Add(4);

        var rows = await SqlNodeRunner.ReadAsync(Source<Snake>(o => o with { Naming = ColumnNamingPolicy.SnakeCaseLower }));

        rows.Single().OrderId.Should().Be(4);
    }

    public enum Status
    {
        Pending,
        Active,
    }

    public sealed record Order
    {
        public int Id { get; init; }
        public Status Status { get; init; }
        public string? Note { get; init; }
    }

    public sealed record Converted
    {
        public int Id { get; init; }
        public decimal Amount { get; init; }
        public DateOnly Day { get; init; }
    }

    public sealed record Positional(int Id, string Name);

    public sealed record Scored
    {
        public int Id { get; init; }
        public int Score { get; init; }
    }

    public sealed class Required
    {
        public required int Id { get; init; }
        public string? Name { get; init; }
    }

    public sealed class Snake
    {
        public int OrderId { get; set; }
    }
}
