using ZstdSharp;

namespace Orleans.Partitioned.Transactional.DynamoDB.Compression;

internal sealed class ZstdDataCompressor : IDataCompressor
{
    private readonly int _level;

    public ZstdDataCompressor(int level = 3)
    {
        _level = level;
    }

    public byte[] Compress(byte[] data)
    {
        using var compressor = new Compressor(_level);
        return compressor.Wrap(data).ToArray();
    }

    public byte[] Decompress(byte[] data)
    {
        using var decompressor = new Decompressor();
        return decompressor.Unwrap(data).ToArray();
    }
}
