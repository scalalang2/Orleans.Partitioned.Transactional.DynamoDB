namespace Orleans.Partitioned.Transactional.DynamoDB.Compression;

/// <summary>
/// Compresses and decompresses binary data.
/// </summary>
public interface IDataCompressor
{
    byte[] Compress(byte[] data);
    byte[] Decompress(byte[] data);
}
