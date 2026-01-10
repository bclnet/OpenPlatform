using Microsoft.AspNetCore.Mvc;
using System.Web;
using static Open.Services.Globals;
namespace Open.Services.Oauth2;

/// <summary>
/// AuthorizeController
/// </summary>
[ApiController, Route("services/oauth2/[controller]")]
public class AuthorizeController : ControllerBase {
    public class Req {
        public required string state { get; set; }          // f42f1ebcb90e
        public required string prompt { get; set; }         // login
        public required string scope { get; set; }          // refresh_token api web
        public required string code_challenge { get; set; } // 59P9WmIiMyjmfBYb2mEiRC5Y44giSIcGDlLYrIbKd5Y
        public required string response_type { get; set; }  // code
        public required string client_id { get; set; }      // PlatformCLI
        public required string? client_secret { get; set; } // {optional}
        public required Uri redirect_uri { get; set; }      // http://localhost:1717/OauthRedirect
    }

    [HttpGet]
    public IActionResult Get([FromQuery] Req req) {
        var builder = new UriBuilder(req.redirect_uri);
        var query = HttpUtility.ParseQueryString(builder.Query);
        var (error, errorDesc) = Oauth2X.Authorize(req.client_id, req.client_secret, req.scope);
        if (error != null) { query["error"] = error; query["error_description"] = errorDesc; }
        else { query["code"] = req.code_challenge; query["state"] = req.state; }
        builder.Query = query.ToString();
        var uri = builder.Uri.ToString();
        return Redirect(uri);
    }
}

/// <summary>
/// TokenController
/// </summary>
[ApiController, Route("services/oauth2/[controller]")]
public class TokenController : ControllerBase {
    public class Req {
        public required string grant_type { get; set; }         // authorization_code
        public required string code { get; set; }               // AE_NN0M7sO7BYOsLxeAJQHFqiVtYVrW6xtVY74m6n2s
        public required string client_id { get; set; }          // PlatformCLI
        public required string? client_secret { get; set; }     // {optional}
        public required Uri redirect_uri { get; set; }          // http://localhost:1717/OauthRedirect
        public required string code_verifier { get; set; }      // z8ko1KPq4jHbAE_fMbW3dMPMoa0ViOCdtsEV7WoNstXQ6dxke-A4EiMbimOy9a4m-p_Zo0od72b8xWnE4NQTvhjhlgBCNDTpEWQnaZy6Kbd1FDaXRwOLl6ILTkNvy365Zm-kEcu2QRErGYYhWXSwhhRv8sl4ynGxPzRp7hbvERU
    }

    public class Res {
        public string token_type { get; set; } = "Bearer";
        /// <summary>
        /// Space-separated list of OAuth scopes associated with the access token
        /// For the OAuth 2.0 Web Server Flow, this can be a subset of the registered scopes if specified when requesting the auth code.
        /// </summary>
        /// <see cref="https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_tokens_scopes.htm&type=5"/>
        public required string scope { get; set; }
        /// <summary>
        /// Identity URL
        /// The format of the URL is https://login.salesforce.com/id/orgID/userID.
        /// </summary>
        public required string id { get; set; }
        public required string access_token { get; set; }
        public string? refresh_token { get; set; }  // code
        public required string signature { get; set; }
        public required string issued_at { get; set; }
        public required string instance_url { get; set; }
        public string? sfdc_community_url { get; set; }
        public string? sfdc_community_id { get; set; }
    }

    [HttpPost]
    public Res Post([FromForm] Req req) {
        return new() {
            scope = "refresh_token api web",
            id = $"https://localhost:7019/id/{ORGID}/{USERID}",
            access_token = "accessToken",
            refresh_token = "refreshToken",
            signature = "signature",
            issued_at = "issued_at",
            instance_url = "https://localhost:7019",
            sfdc_community_url = "sfdc_community_url",
            sfdc_community_id = "sfdc_community_id",
        };
    }
}

/// <summary>
/// UserInfoController
/// </summary>
[ApiController, Route("services/oauth2/[controller]")]
public class UserInfoController : ControllerBase {
    public class Res {
        public required string preferred_username { get; set; }
        public required string organization_id { get; set; }
        public required string user_id { get; set; }
    }

    [HttpGet]
    public Res Get() {
        var authorization = Request.Headers["Authorization"];
        return new() {
            preferred_username = USERNAME,
            organization_id = ORGID,
            user_id = USERID,
        };
    }
}

/// <summary>
/// RevokeController
/// </summary>
[ApiController, Route("services/oauth2/[controller]")]
public class RevokeController : ControllerBase {
    [HttpGet]
    public IActionResult Get() => throw new NotImplementedException();
}