using Confluent.Kafka;

namespace Producer;

public class ProducerFactory
{
    private static readonly ProducerConfig _producerConfig = new ProducerConfig
    {
        BootstrapServers = "localhost:9092"
    };
    public static IProducer<TKey, TValue> GetProducer<TKey, TValue>()
    {
        var producerBuilder = new ProducerBuilder<TKey, TValue>(_producerConfig);
        producerBuilder.SetValueSerializer(new Serializer<TValue>());
        return producerBuilder.Build();
    }
}