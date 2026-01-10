using Microsoft.AspNetCore.Mvc;
namespace Open.Services.Data;

/// <summary>
/// QueryController
/// </summary>
[ApiController, Route("services/data/{apiVersion}/res")]
public class QueryController : ControllerBase {
    public class Res {
        public required int totalSize { get; set; }     // 1
        public required bool done { get; set; }         // true
        public string? nextRecordsUrl { get; set; }     // "/services/data/v51.0/query/0r8xx50ZnWewYSzAUM-2000"
        public required IEnumerable<SObject> records { get; set; }
    }

    [HttpGet]
    public object Get(string apiVersion, [FromQuery] string q) {
        var res = DataX.Query(apiVersion, q);
        if (res == null || res[0] is ApiError) return NotFound(res ?? [new ApiError("Query returned null", "NULL")]);
        return new Res() {
            totalSize = res.Length,
            done = true,
            records = res.Cast<SObject>()
        };
    }
}

/// <summary>
/// SObjectsController
/// </summary>
[ApiController, Route("services/data/{apiVersion}/[controller]/{sobject}/{id}")]
public class SObjectsController : ControllerBase {
    [HttpGet]
    public object Get(string apiVersion, string sobject, string id) {
        var res = DataX.Get(apiVersion, sobject, id);
        if (res == null || res[0] is ApiError) return NotFound(res ?? [new ApiError("Get returned null", "NULL")]);
        return res[0];
    }
}
