namespace Open.Services;

public class Globals {
    public static string ORGID = "orgID";
    public static string USERID = "userID";
    public static string USERNAME = "username";
}

public class ApiError(string message, string errorCode) {
    public string message { get; } = message;   // error message
    public string errorCode { get; } = errorCode; // INVALID_TYPE
}

public class SObject : Dictionary<string, object> {
    public string id { get; }

    public SObject(string apiVersion, string sobject, string id) {
        this.id = id;
        this["attributes"] = new Dictionary<string, object> {
            ["type"] = sobject,
            ["url"] = $"/services/data/{apiVersion}/sobjects/{sobject}/{id}"
        };
    }
}
