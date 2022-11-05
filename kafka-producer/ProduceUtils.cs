using Confluent.Kafka;

namespace Producer;

public static class ProduceUtils
{
    public static async Task<bool> Produce<TKey, TValue>(IProducer<TKey, TValue> producer, TKey key, TValue value, string topic)
    {

        var record = new Message<TKey, TValue>
        {
            Key = key,
            Value = value,
        };
        var result = await producer.ProduceAsync(topic, record);
        return result.Status == PersistenceStatus.Persisted;
    }
}