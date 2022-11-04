using Confluent.Kafka;

namespace Producer;

public class KafkaProducer
{
    private ProducerConfig just = new ProducerConfig
    {
        BootstrapServers = "localhost:9092"
    };
    public bool Produce<TKey, TValue>(TKey key, TValue value, string topic)
    {
        using (var producer = new ProducerBuilder<TKey, TValue>(just).Build())
        {
            bool result = false;
            var record = new Message<TKey, TValue>
            {
                Key = key,
                Value = value,
                Timestamp = Timestamp.Default
            };
            producer.Produce(
                topic,
                record,
                deliveryReport =>
                {
                    result = deliveryReport.Error.Code == ErrorCode.NoError;
                }
            );
            return result;
        }
    }
}