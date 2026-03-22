using System.Collections;
using System.Security.Cryptography;
using System.Text;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

internal static class PartitionedStateHelper
{
    public static (Type keyType, Type valueType) GetPartitionTypeArgs(Type stateType)
    {
        var iface = stateType.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IPartitionedState<,>));

        if (iface == null)
        {
            throw new InvalidOperationException($"Type {stateType} doesn't implement IPartitionedState<TKey, TValue>");
        }

        // since it's implemented with IPartitioned<,> interface,
        // the length of returned arguments always is 2. 
        var args = iface.GetGenericArguments();
        return (args[0], args[1]);
    }

    public static bool IsPartitionedState(Type stateType)
    {
        return stateType.GetInterfaces()
            .Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IPartitionedState<,>));
    }

    public static uint AssignToPartition<TKey>(TKey key, int partitionSize)
    {
        var hash = DeterministicHash(key);
        return (hash % (uint)partitionSize) + 1;
    }
    
    private static uint DeterministicHash(object key)
    {
        var bytes = Encoding.UTF8.GetBytes(key.ToString()!);
        var hash = SHA256.HashData(bytes);
        return BitConverter.ToUInt32(hash, 0);
    }

    public static Dictionary<uint, IDictionary> SplitIntoPartitions(IDictionary items, int partitionSize, Type dictType)
    {
        var result = new Dictionary<uint, IDictionary>();

        if (items == null || items.Count == 0)
            return result;

        foreach (DictionaryEntry entry in items)
        {
            var partNum = AssignToPartition(entry.Key, partitionSize);
            if (!result.TryGetValue(partNum, out var partition))
            {
                partition = (IDictionary)Activator.CreateInstance(dictType)!;
                result[partNum] = partition;
            }
            partition[entry.Key] = entry.Value;
        }

        return result;
    }

    public static string ComputeHash(byte[] data)
    {
        if (data == null || data.Length == 0)
        {
            return string.Empty;
        }

        var hash = SHA256.HashData(data);
        return Convert.ToHexString(hash);
    }
}