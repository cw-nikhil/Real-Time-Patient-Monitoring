using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Producer.Models;

namespace Producer.Controllers;

[ApiController]
[Route("api/heartbeat")]
public class HeartbeatController : ControllerBase
{
    private const string hbTopic = "raw-heartbeat-topic";
    private readonly IProducer<string, Heartbeat> _producer;

    public HeartbeatController(IProducer<string, Heartbeat> producer)
    {
        _producer = producer;
    }

    [HttpPost("register")]
    public async Task<IActionResult> RegisterHeartbeat(Heartbeat heartbeat)
    {
        if (string.IsNullOrEmpty(heartbeat.DeviceId) || string.IsNullOrEmpty(heartbeat.PatientId))
        {
            return BadRequest("invalid patientId or deviceId");
        }
        string recordKey = $"{heartbeat.PatientId}-{heartbeat.DeviceId}";
        bool result = await ProduceUtils.Produce<string, Heartbeat>(_producer, recordKey, heartbeat, hbTopic);
        return result ? Ok() : new StatusCodeResult(StatusCodes.Status500InternalServerError);
    }
}