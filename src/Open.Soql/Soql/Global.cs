using System.Data;

namespace Open.Soql;

public static class Globalx {
    public static string PascalCaseToSnakeCase(string name) => string.Concat(name.Select((c, i) => i > 0 && char.IsUpper(c) ? "_" + char.ToLower(c) : char.ToLower(c).ToString()));
    public static string SnakeCaseToPascalCase(string name) => string.Join("", name.Split('_').Select(s => char.ToUpper(s[0]) + s[1..].ToLower()));
}

public class SoqlParseException(string message) : Exception(message) { }
