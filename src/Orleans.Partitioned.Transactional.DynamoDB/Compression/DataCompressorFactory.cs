using System;

namespace Orleans.Partitioned.Transactional.DynamoDB.Compression;

internal static class DataCompressorFactory
{
    public static IDataCompressor Create(CompressionAlgorithm algorithm) => algorithm switch
    {
        CompressionAlgorithm.Zstd => new ZstdDataCompressor(),
        CompressionAlgorithm.Gzip => new GzipDataCompressor(),
        CompressionAlgorithm.Zlib => new ZlibDataCompressor(),
        _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unsupported compression algorithm.")
    };
}
