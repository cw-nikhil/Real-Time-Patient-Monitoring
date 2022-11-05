using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Producer;

public class Serializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        string stringForm = JsonSerializer.Serialize<T>(
            data,
            new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            }
        );
        return Encoding.UTF8.GetBytes(stringForm);
    }
}