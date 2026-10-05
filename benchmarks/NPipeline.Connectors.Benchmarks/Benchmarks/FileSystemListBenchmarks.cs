using BenchmarkDotNet.Attributes;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Benchmarks.Benchmarks;

/// <summary>
///     Lists 10,000 files, flat and in a nested tree. <c>PerEntryUri</c> is the walk the provider used before FS-3 (a
///     <c>FileSystemInfo</c> per entry and a path resolution per URI); <c>Provider</c> is the current single enumeration.
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
public class FileSystemListBenchmarks
{
    private const int Files = 10_000;

    private readonly FileSystemStorageProvider _provider = new();
    private string _flat = null!;
    private string _nested = null!;

    [GlobalSetup]
    public void Setup()
    {
        var root = Path.Combine(Path.GetTempPath(), $"npipeline-list-bench-{Guid.NewGuid():N}");
        _flat = Path.Combine(root, "flat");
        _nested = Path.Combine(root, "nested");
        _ = Directory.CreateDirectory(_flat);

        for (var i = 0; i < Files; i++)
            File.WriteAllBytes(Path.Combine(_flat, $"f{i}.dat"), []);

        for (var i = 0; i < Files; i++)
        {
            var dir = Path.Combine(_nested, $"d{i % 10}", $"e{i % 50}", $"g{i % 7}");
            _ = Directory.CreateDirectory(dir);
            File.WriteAllBytes(Path.Combine(dir, $"f{i}.dat"), []);
        }
    }

    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(Path.GetDirectoryName(_flat)!, true);

    [Benchmark]
    public int Flat_PerEntryUri() => Legacy(_flat, false);

    [Benchmark]
    public async Task<int> Flat_Provider() => await Count(_flat, false);

    [Benchmark]
    public int Nested_PerEntryUri() => Legacy(_nested, true);

    [Benchmark]
    public async Task<int> Nested_Provider() => await Count(_nested, true);

    private async Task<int> Count(string path, bool recursive)
    {
        var n = 0;

        await foreach (var _ in _provider.ListAsync(StorageUri.FromFilePath(path), recursive))
            n++;

        return n;
    }

    private static int Legacy(string root, bool recursive)
    {
        var directory = StorageUri.FromFilePath(root);
        var pending = new Stack<string>();
        pending.Push(root);
        var n = 0;

        while (pending.Count > 0)
        {
            foreach (var entry in new DirectoryInfo(pending.Pop()).EnumerateFileSystemInfos("*", SearchOption.TopDirectoryOnly))
            {
                var isDirectory = (entry.Attributes & FileAttributes.Directory) != 0;

                if (isDirectory && recursive)
                {
                    if ((entry.Attributes & FileAttributes.ReparsePoint) == 0)
                        pending.Push(entry.FullName);

                    continue;
                }

                var path = StorageUri.FromFilePath(entry.FullName).Path;
                var item = new StorageItem
                {
                    Uri = directory.WithPath(isDirectory ? path + "/" : path),
                    Size = isDirectory ? null : ((FileInfo)entry).Length,
                    LastModified = entry.LastWriteTimeUtc,
                    IsDirectory = isDirectory,
                };

                n += item.Uri is null ? 0 : 1;
            }
        }

        return n;
    }
}
