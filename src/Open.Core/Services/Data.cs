using static Open.Services.Globals;

namespace Open.Services.Data;

public static class DataX {
    public static object[] Get(string apiVersion, string sobject, string id) => sobject switch {
        "User" => [new SObject(apiVersion, sobject, id) {
            ["Id"] = id,
            ["Username"] = "username"
        }],
        _ => [new ApiError("Bad res", "INVALID_TYPE")]
    };

    public static object[] Query(string apiVersion, string q) => q switch {
        "SELECT Id FROM ScratchOrgInfo limit 1" => [new ApiError("Bad res", "INVALID_TYPE")],
        "Select Namespaceprefix FROM Organization" => [new SObject(apiVersion, "Organization", ORGID) { ["NamespacePrefix"] = null! }],
        _ => [new ApiError("Bad res", "INVALID_TYPE")]
    };
}