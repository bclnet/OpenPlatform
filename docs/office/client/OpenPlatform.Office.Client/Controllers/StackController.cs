using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using OpenPlatform.Office.Client.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OpenPlatform.Office.Client.Controllers
{
    [ApiController, Route("[controller]")]
    public class StackController : ControllerBase
    {
        static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        readonly ILogger<StackController> _logger;

        public StackController(ILogger<StackController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public IEnumerable<Stack> Get()
        {
            var rng = new Random();
            return Enumerable.Range(1, 5).Select(index => new Stack
            {
                CreatedOn = DateTime.Now.AddDays(index),
                Id = rng.Next(1, 100).ToString(),
                Name = Summaries[rng.Next(Summaries.Length)]
            })
            .ToArray();
        }
    }
}
