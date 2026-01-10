
namespace Open.Services.Oauth2;

public static class Oauth2X {
    public static (string, string) Authorize(string clientId, string? clientSecret, string scope) {
        if (scope == "throw") return ("Error Header", "Error Body");
        return default!;
    }
}
