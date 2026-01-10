using Microsoft.AspNetCore.HttpLogging;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers()
    .AddJsonOptions(options => {
        options.JsonSerializerOptions.DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull;
    });
builder.Services.AddHttpLogging(logging => {
    logging.LoggingFields = HttpLoggingFields.All;
    logging.RequestBodyLogLimit = 4096; // Optional: set body size limit
    logging.ResponseBodyLogLimit = 4096;
    logging.CombineLogs = true; // Optional: combine request/response into one log entry
});

var app = builder.Build();
app.Use(async (context, next) => {
    var value = context.Request.Path.Value;
    if (value?.StartsWith("//") == true) context.Request.Path = new PathString(value[1..]);
    await next.Invoke();
});

// Configure the HTTP request pipeline.
//app.UseRewriter(new RewriteOptions().AddRewrite(@"^//+(.*)", "/$1", skipRemainingRules: true));
app.UseHttpLogging();
//app.UseAuthorization();
app.MapGet("/", () => "Hello World! Welcome to the API.");
app.UseRouting();
app.MapControllers();
app.Run();
