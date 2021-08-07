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
    public class ShardController : ControllerBase
    {
        static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        readonly ILogger<ShardController> _logger;

        public ShardController(ILogger<ShardController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public IEnumerable<Shard> Get()
        {
            var rng = new Random();
            return Enumerable.Range(1, 5).Select(index => new Shard
            {
                CreatedOn = DateTime.Now.AddDays(index),
                Id = rng.Next(1, 100).ToString(),
                Name = Summaries[rng.Next(Summaries.Length)]
            })
            .ToArray();
        }
    }
}
