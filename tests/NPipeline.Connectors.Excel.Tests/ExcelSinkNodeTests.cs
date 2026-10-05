using AwesomeAssertions;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using DocumentFormat.OpenXml.Validation;
using NPipeline.Connectors.Files;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Excel.Tests;

public sealed class ExcelSinkNodeTests : ExcelTestBase
{
    private static long PendingBytes(string path) =>
        Directory.GetFiles(Path.GetDirectoryName(path)!, $".{Path.GetFileName(path)}.*.tmp").Sum(file => new FileInfo(file).Length);

    private static readonly TypedRow Typed = new()
    {
        Big = 9_007_199_254_740_993,
        Ratio = 0.1 + 0.2,
        Precise = 0.1234567890123456789m,
        Flag = true,
        At = new DateTime(2026, 1, 2, 3, 4, 5, 678, DateTimeKind.Utc),
        Offset = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromHours(10)),
        Day = new DateOnly(2026, 1, 2),
        Time = new TimeOnly(13, 14, 15),
        Span = new TimeSpan(1, 2, 3, 4),
        Key = Guid.Parse("6f9619ff-8b86-d011-b42d-00cf4fc964ff"),
        Tier = Tier.Pro,
    };

    [Fact]
    public async Task Writes_a_bold_header_of_member_names_and_one_row_per_item()
    {
        await WriteAsync(Sink<Person>(), new Person { Id = 1, Name = "Ada", Balance = 12.5m });

        using var document = Open();
        Cells(document).Should().BeEquivalentTo(
            [new[] { "Id", "Name", "Balance", "Country" }, ["1", "Ada", "12.5", "AU"]],
            o => o.WithStrictOrdering());
        HeaderCell(document).StyleIndex!.Value.Should().Be(4);
    }

    [Fact]
    public async Task Writes_typed_cells_that_read_back_unchanged()
    {
        await WriteAsync(Sink<TypedRow>(), Typed);

        (await ReadAsync(Source<TypedRow>())).Single().Should().BeEquivalentTo(Typed);
    }

    [Fact]
    public async Task Numbers_dates_and_booleans_are_typed_cells_and_dates_are_formatted()
    {
        await WriteAsync(Sink<TypedRow>(), Typed);

        using var document = Open();
        var cells = document.WorkbookPart!.WorksheetParts.Single().Worksheet!.Descendants<Row>().ElementAt(1).Elements<Cell>().ToList();
        var byColumn = cells.ToDictionary(c => new string(c.CellReference!.Value!.TakeWhile(char.IsLetter).ToArray()));

        byColumn["A"].DataType!.Value.Should().Be(CellValues.InlineString, "a long beyond 2^53 is text, so no digit is lost");
        byColumn["B"].DataType.Should().BeNull("a double is a number");
        byColumn["C"].DataType!.Value.Should().Be(CellValues.InlineString, "a decimal a double cannot hold exactly is text");
        byColumn["D"].DataType!.Value.Should().Be(CellValues.Boolean);
        byColumn["E"].StyleIndex!.Value.Should().Be(2, "a date and time has a date-time format");
        byColumn["G"].StyleIndex!.Value.Should().Be(1, "a date has a date format");
        byColumn["H"].StyleIndex!.Value.Should().Be(3, "a time has a time format");
        document.WorkbookPart!.WorkbookStylesPart.Should().NotBeNull();
    }

    [Fact]
    public async Task Produces_a_valid_workbook_with_a_frozen_filtered_header()
    {
        await WriteAsync(Sink<TypedRow>(o => o with { FreezeHeader = true, AutoFilter = true, SheetName = "Q3 'Orders' & more" }), Typed, Typed);

        using var document = Open();
        new OpenXmlValidator().Validate(document).Select(e => $"{e.Path?.XPath}: {e.Description}").Should().BeEmpty();
        document.WorkbookPart!.Workbook!.Sheets!.Elements<Sheet>().Single().Name!.Value.Should().Be("Q3 'Orders' & more");
        var worksheet = document.WorkbookPart.WorksheetParts.Single().Worksheet!;
        worksheet.Descendants<Pane>().Single().State!.Value.Should().Be(PaneStateValues.Frozen);
        worksheet.Elements<AutoFilter>().Single().Reference!.Value.Should().Be("A1:L3");
    }

    [Fact]
    public async Task Writes_to_streams_that_cannot_seek()
    {
        var provider = new InMemoryStorageProvider { NonSeekableWrites = true };

        await WriteAsync(ExcelConnector.Sink<Person>(Uri(), o => o with { Provider = provider }), new Person { Id = 1, Name = "Ada" });

        using var document = SpreadsheetDocument.Open(new MemoryStream(provider.Get(Uri())), false);
        new OpenXmlValidator().Validate(document).Should().BeEmpty();
    }

    [Fact]
    public async Task Keeps_whitespace_line_breaks_and_characters_xml_cannot_hold()
    {
        string[] texts = ["  padded  ", "crlf\r\nbreak", "bell\u0007", "looks_x0041_escaped", "emoji 🚀"];

        await WriteAsync(Sink<string>(), texts);

        (await ReadAsync(Source<string>())).Should().Equal(texts);
    }

    [Fact]
    public async Task A_scalar_type_writes_one_column_without_a_header()
    {
        await WriteAsync(Sink<int>(), 3, 1);

        using var document = Open();
        Cells(document).Should().BeEquivalentTo([new[] { "3" }, ["1"]], o => o.WithStrictOrdering());
    }

    [Fact]
    public async Task Null_values_are_empty_cells_and_null_items_fail_by_default()
    {
        var fail = () => WriteAsync(Sink<Person?>(path: "failed.xlsx"), new Person { Id = 1 }, null);

        await WriteAsync(Sink<TypedRow>(), Typed);
        await fail.Should().ThrowAsync<InvalidOperationException>();

        (await ReadAsync(Source<TypedRow>())).Single().Missing.Should().BeNull();
    }

    [Fact]
    public async Task Text_longer_than_a_cell_holds_fails_naming_the_cell()
    {
        var write = () => WriteAsync(Sink<Person>(), new Person { Id = 1, Name = new string('x', 32_768) });

        (await write.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*cell B2 has 32,768 characters*");
    }

    [Fact]
    public async Task A_manual_writer_writes_the_given_columns()
    {
        var sink = ExcelConnector.Sink<Person>(
            Uri(),
            ["Who", "Owes"],
            (row, person) =>
            {
                row.Write(person.Name.ToUpperInvariant());
                row.Write(person.Balance);
            },
            o => o with { Provider = Provider });

        await WriteAsync(sink, new Person { Name = "Ada", Balance = 3m });

        using var document = Open();
        Cells(document).Should().BeEquivalentTo([new[] { "Who", "Owes" }, ["ADA", "3"]], o => o.WithStrictOrdering());
    }

    [Fact]
    public async Task Streams_rows_to_storage_before_the_input_completes()
    {
        var path = Path.Combine(Path.GetTempPath(), $"np_excel_{Guid.NewGuid():N}.xlsx");
        long bytesBeforeLastItem = -1;

        async IAsyncEnumerable<Person> Items()
        {
            for (var i = 0; i < 50_000; i++)
            {
                yield return new Person { Id = i, Name = $"person {i}", Balance = i };
            }

            await Task.Yield();
            bytesBeforeLastItem = PendingBytes(path);
            yield return new Person { Id = 50_000 };
        }

        try
        {
            var sink = ExcelConnector.Sink<Person>(StorageUri.FromFilePath(path));
            await sink.ConsumeAsync(new DataStream<Person>(Items(), "people"), new PipelineContext(), CancellationToken.None);

            bytesBeforeLastItem.Should().BeGreaterThan(200_000, "rows must reach storage as they are written");
            (await ReadAsync(ExcelConnector.Source<Person>(StorageUri.FromFilePath(path)))).Should().HaveCount(50_001);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("a/b")]
    [InlineData("[x]")]
    [InlineData("'quoted'")]
    [InlineData("History")]
    [InlineData("a name that is longer than 31 chars")]
    public void Rejects_invalid_sheet_names(string name)
    {
        var create = () => Sink<Person>(o => o with { SheetName = name });

        create.Should().Throw<ArgumentException>().WithParameterName("SheetName");
    }

    [Fact]
    public async Task Rejects_stream_compression_because_the_format_compresses_itself()
    {
        var write = () => WriteAsync(Sink<Person>(o => o with { Compression = FileCompression.Gzip }), new Person());

        await write.Should().ThrowAsync<NotSupportedException>();
    }

    private static List<string[]> Cells(SpreadsheetDocument document)
    {
        var worksheet = document.WorkbookPart!.WorksheetParts.Single().Worksheet!;

        return worksheet.Descendants<Row>()
            .Select(row => row.Elements<Cell>().Select(c => c.DataType?.Value == CellValues.InlineString ? c.InlineString!.InnerText : c.CellValue!.Text).ToArray())
            .ToList();
    }

    private static Cell HeaderCell(SpreadsheetDocument document) =>
        document.WorkbookPart!.WorksheetParts.Single().Worksheet!.Descendants<Cell>().First();
}
