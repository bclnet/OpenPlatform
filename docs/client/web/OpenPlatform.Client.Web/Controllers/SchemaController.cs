using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using OpenPlatform.Client.Web.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenPlatform.Client.Web.Controllers
{
    [ApiController, Route("[controller]")]
    public class SchemaController : ControllerBase
    {
        static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        readonly ILogger<SchemaController> _logger;

        public SchemaController(ILogger<SchemaController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public IEnumerable<Schema> Get()
        {
            var rng = new Random();
            return Enumerable.Range(1, 5).Select(index => new Schema
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = rng.Next(-20, 55),
                Summary = Summaries[rng.Next(Summaries.Length)]
            })
            .ToArray();
        }
    }
}
