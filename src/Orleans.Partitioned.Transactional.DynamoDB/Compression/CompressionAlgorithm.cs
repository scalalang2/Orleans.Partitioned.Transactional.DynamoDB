namespace Orleans.Partitioned.Transactional.DynamoDB.Compression;

/// <summary>
/// Supported compression algorithms for transactional state data.
/// </summary>
public enum CompressionAlgorithm
{
    Zstd,
    Gzip,
    Zlib,
}
