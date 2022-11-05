using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Producer.Models;

namespace Producer.Controllers;

[ApiController]
[Route("api/bp")]
public class BloodPressureController : ControllerBase
{
    private const string bpTopic = "blood-pressure-topic";
    private readonly IProducer<string, BloodPressure> _producer;

    public BloodPressureController(IProducer<string, BloodPressure> producer)
    {
        _producer = producer;
    }
    [HttpPost("register")]
    public async Task<IActionResult> RegisterBloodPressure(BloodPressure bloodPressure)
    {
        if (string.IsNullOrEmpty(bloodPressure.PatientId))
        {
            return BadRequest("invalid patientId");
        }
        if (bloodPressure.SystolicPressure == 0 || bloodPressure.DiastolicPressure == 0) {
            return BadRequest("Pressure values cannot be 0");
        }
        bool result = await ProduceUtils.Produce<string, BloodPressure>(_producer, bloodPressure.PatientId, bloodPressure, bpTopic);
        return result ? Ok() : new StatusCodeResult(StatusCodes.Status500InternalServerError);
    }
}