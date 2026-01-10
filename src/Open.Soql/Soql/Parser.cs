using Open.Soql.Metadata;
using System.Text.RegularExpressions;

namespace Open.Soql.Parser;

#region Models

/// <summary>
/// Represents a parsed SOQL query with all its components
/// </summary>
public class SoqlQuery {
    public required List<SoqlField> SelectFields { get; set; } = [];
    public required string FromObject { get; set; }
    public SoqlCondition? WhereClause { get; set; }
    public List<SoqlOrderBy>? OrderBy { get; set; }
    public int? Limit { get; set; }
    public int? Offset { get; set; }
    public List<SoqlJoin>? Joins { get; set; }
    public List<string>? GroupBy { get; set; }
    public SoqlCondition? HavingClause { get; set; }
    public bool IsAggregate => SelectFields.Any(f => f.IsAggregate);
    public List<SoqlQuery>? SubQueries { get; set; }
}

/// <summary>
/// Represents a field in the SELECT clause
/// </summary>
public class SoqlField {
    public string? FieldName { get; set; }
    public string? Alias { get; set; }
    public bool IsAggregate { get; set; }
    public AggregateFunction? AggregateType { get; set; }
    public string? AggregateField { get; set; }
    public SoqlQuery? SubQuery { get; set; }

    public string? GetSqlExpression(string? tableAlias = null)
        => SubQuery != null ? null // Handled separately
        : IsAggregate ? $"{AggregateType}({(string.IsNullOrEmpty(AggregateField) ? "*" : (tableAlias != null ? $"{tableAlias}.{AggregateField}" : AggregateField))})"
        : tableAlias != null ? $"{tableAlias}.{FieldName}" : FieldName;
}

public enum AggregateFunction { COUNT, SUM, AVG, MIN, MAX, COUNT_DISTINCT }

/// <summary>
/// Represents a WHERE or HAVING condition
/// </summary>
public class SoqlCondition {
    public string? Field { get; set; }
    public SoqlOperator Operator { get; set; }
    public object? Value { get; set; }
    public LogicalOperator LogicalOp { get; set; }
    public SoqlCondition? Left { get; set; }
    public SoqlCondition? Right { get; set; }
    public bool IsCompound => Left != null || Right != null;
    public SoqlQuery? SubQuery { get; set; }
}

public enum SoqlOperator { Equals, NotEquals, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Like, In, NotIn, IsNull, IsNotNull, Contains, StartsWith, EndsWith }

public enum LogicalOperator { AND, OR }

/// <summary>
/// Represents an ORDER BY clause
/// </summary>
public class SoqlOrderBy {
    public required string Field { get; set; }
    public SortDirection Direction { get; set; } = SortDirection.ASC;
    public NullsOrder NullsOrder { get; set; } = NullsOrder.Last;
}

public enum SortDirection { ASC, DESC }

public enum NullsOrder { First, Last }

/// <summary>
/// Represents a JOIN operation
/// </summary>
public class SoqlJoin {
    public required string RelationshipName { get; set; }
    public required string TargetObject { get; set; }
    public string? ForeignKey { get; set; }
    public string? PrimaryKey { get; set; }
    public JoinType Type { get; set; } = JoinType.LEFT;
    public int EstimatedRowCount { get; set; }
    public double Selectivity { get; set; } = 1.0;
}

public enum JoinType { INNER, LEFT, RIGHT }

#endregion

#region Parse

/// <summary>
/// Parses SOQL query strings into structured query models
/// Example: SELECT Id, Name, Account.Name FROM Contact WHERE CreatedDate > 2024-01-01 ORDER BY Name LIMIT 10
/// </summary>
public partial class SoqlParser(IMetadataProvider metadataProvider) {
    readonly IMetadataProvider _metadataProvider = metadataProvider;

    public SoqlQuery Parse(string soql) {
        if (string.IsNullOrWhiteSpace(soql)) throw new ArgumentException("SOQL s cannot be empty");
        soql = soql.Trim();
        // extract main clauses using regex
        var selectMatch = SELECT().Match(soql);
        var fromMatch = FROM().Match(soql);
        var whereMatch = WHERE().Match(soql);
        var orderMatch = ORDERBY().Match(soql);
        var groupMatch = GROUPBY().Match(soql);
        var havingMatch = HAVING().Match(soql);
        var limitMatch = LIMIT().Match(soql);
        var offsetMatch = OFFSET().Match(soql);
        if (!selectMatch.Success || !fromMatch.Success) throw new SoqlParseException("Invalid SOQL: Must have SELECT and FROM clauses");
        var query = new SoqlQuery {
            FromObject = fromMatch.Groups[1].Value.Trim(),
            SelectFields = ParseSelectFields(selectMatch.Groups[1].Value),
            WhereClause = whereMatch.Success ? ParseCondition(whereMatch.Groups[1].Value.Trim()) : null,
            GroupBy = groupMatch.Success ? [.. groupMatch.Groups[1].Value.Split(',').Select(f => f.Trim())] : null,
            HavingClause = havingMatch.Success ? ParseCondition(havingMatch.Groups[1].Value.Trim()) : null,
            OrderBy = orderMatch.Success ? ParseOrderBy(orderMatch.Groups[1].Value) : null,
            Limit = limitMatch.Success ? int.Parse(limitMatch.Groups[1].Value) : null,
            Offset = offsetMatch.Success ? int.Parse(offsetMatch.Groups[1].Value) : null,
        };
        ResolveRelationships(query); // Resolve relationships and build joins
        return query;
    }

    List<SoqlField> ParseSelectFields(string s) {
        var res = new List<SoqlField>();
        foreach (var part in SplitRespectingParentheses(s, ',')) {
            var trimmed = part.Trim();
            // check for subquery (SELECT ... FROM ...)
            if (trimmed.StartsWith("(", StringComparison.OrdinalIgnoreCase)) {
                var subQuerySoql = trimmed[1..^1];
                var subQuery = Parse(subQuerySoql);
                res.Add(new SoqlField { SubQuery = subQuery, FieldName = $"SubQuery_{res.Count}" });
                continue;
            }
            // check for aggregate functions
            var aggMatch = AGGREGATE().Match(trimmed);
            if (aggMatch.Success) {
                var aggType = Enum.Parse<AggregateFunction>(aggMatch.Groups[1].Value.ToUpper());
                var aggField = aggMatch.Groups[2].Value.Trim();
                var alias = aggMatch.Groups[3].Success ? aggMatch.Groups[3].Value : null;
                if (aggType == AggregateFunction.COUNT && aggField.Equals("DISTINCT", StringComparison.CurrentCultureIgnoreCase)) {
                    aggType = AggregateFunction.COUNT_DISTINCT;
                    aggField = trimmed[(trimmed.IndexOf("DISTINCT", StringComparison.OrdinalIgnoreCase) + 8)..].Trim().TrimEnd(')');
                }
                res.Add(new SoqlField {
                    IsAggregate = true,
                    AggregateType = aggType,
                    AggregateField = aggField == "*" ? null : aggField,
                    Alias = alias,
                    FieldName = alias ?? $"{aggType}_{aggField}"
                });
                continue;
            }
            // regular field (may include relationship traversal like Account.Name)
            var aliasParts = trimmed.Split([" AS ", " "], StringSplitOptions.RemoveEmptyEntries);
            var fieldName = aliasParts[0].Trim();
            var alias2 = aliasParts.Length > 1 ? aliasParts[^1].Trim() : null;
            res.Add(new SoqlField { FieldName = fieldName, Alias = alias2 });
        }
        return res;
    }

    SoqlCondition? ParseCondition(string s) {
        if (string.IsNullOrWhiteSpace(s)) return null;
        s = s.Trim();
        if (s.StartsWith('(') && s.EndsWith(')')) return ParseCondition(s[1..^1]); // handle parentheses for grouping
        // look for logical operators (AND, OR) at the top level
        int andIndex = FindTopLevelOperator(s, "AND"), orIndex = FindTopLevelOperator(s, "OR");
        if (andIndex > 0 || orIndex > 0) {
            int splitIndex; LogicalOperator logicalOp;
            // determine which operator comes first
            if (andIndex > 0 && (orIndex < 0 || andIndex < orIndex)) { splitIndex = andIndex; logicalOp = LogicalOperator.AND; }
            else { splitIndex = orIndex; logicalOp = LogicalOperator.OR; }
            return new SoqlCondition {
                LogicalOp = logicalOp,
                Left = ParseCondition(s[..splitIndex].Trim()),
                Right = ParseCondition(s[(splitIndex + (logicalOp == LogicalOperator.AND ? 3 : 2))..].Trim())
            };
        }
        // parse single condition
        return ParseSingleCondition(s);
    }

    SoqlCondition ParseSingleCondition(string s) {
        // Handle IN and NOT IN with subqueries or lists
        var inMatch = IN().Match(s);
        if (inMatch.Success) {
            var field = inMatch.Groups[1].Value;
            var isNotIn = !string.IsNullOrEmpty(inMatch.Groups[2].Value);
            var inValues = inMatch.Groups[3].Value.Trim();
            // check if it's a subquery
            return inValues.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
                ? new SoqlCondition { Field = field, Operator = isNotIn ? SoqlOperator.NotIn : SoqlOperator.In, SubQuery = Parse(inValues) }
                : new SoqlCondition { Field = field, Operator = isNotIn ? SoqlOperator.NotIn : SoqlOperator.In, Value = inValues.Split(',').Select(v => ParseValue(v.Trim())).ToList() };
        }

        // Handle IS NULL / IS NOT NULL
        var nullMatch = IS_NULL().Match(s);
        if (nullMatch.Success) return new SoqlCondition { Field = nullMatch.Groups[1].Value, Operator = nullMatch.Groups[2].Success ? SoqlOperator.IsNotNull : SoqlOperator.IsNull };

        // Handle LIKE
        var likeMatch = LIKE().Match(s);
        if (likeMatch.Success) {
            var value = ParseValue(likeMatch.Groups[2].Value.Trim());
            var strValue = value?.ToString() ?? "";
            var op
                = strValue.StartsWith('%') && strValue.EndsWith('%') ? SoqlOperator.Contains
                : strValue.EndsWith('%') ? SoqlOperator.StartsWith
                : strValue.StartsWith('%') ? SoqlOperator.EndsWith
                : SoqlOperator.Like;
            return new SoqlCondition {
                Field = likeMatch.Groups[1].Value,
                Operator = op,
                Value = value
            };
        }

        // Handle standard comparison operators
        var opMatch = OP().Match(s);
        if (opMatch.Success) {
            var field = opMatch.Groups[1].Value;
            var operatorStr = opMatch.Groups[2].Value;
            var valueStr = opMatch.Groups[3].Value.Trim();
            var op = operatorStr switch {
                "=" => SoqlOperator.Equals,
                "!=" or "<>" => SoqlOperator.NotEquals,
                "<" => SoqlOperator.LessThan,
                "<=" => SoqlOperator.LessThanOrEqual,
                ">" => SoqlOperator.GreaterThan,
                ">=" => SoqlOperator.GreaterThanOrEqual,
                _ => throw new SoqlParseException($"Unknown operator: {operatorStr}")
            };
            return new SoqlCondition {
                Field = field,
                Operator = op,
                Value = ParseValue(valueStr)
            };
        }
        throw new SoqlParseException($"Invalid condition: {s}");
    }

    static object? ParseValue(string s) {
        s = s.Trim();
        return
            (s.StartsWith('\'') && s.EndsWith('\'')) || (s.StartsWith('\"') && s.EndsWith('\"')) ? s[1..^1]
            : s.Equals("NULL", StringComparison.OrdinalIgnoreCase) ? null
            : s.Equals("TRUE", StringComparison.OrdinalIgnoreCase) ? true
            : s.Equals("FALSE", StringComparison.OrdinalIgnoreCase) ? false
            : int.TryParse(s, out var intVal) ? intVal
            : double.TryParse(s, out var doubleVal) ? doubleVal
            : DateTime.TryParse(s, out var dateVal) ? dateVal
            : s;
    }

    static List<SoqlOrderBy> ParseOrderBy(string s) {
        var res = new List<SoqlOrderBy>();
        foreach (var part in s.Split(',')) {
            var tokens = part.Trim().Split(' ');
            var orderBy = new SoqlOrderBy { Field = tokens[0] };
            for (var i = 1; i < tokens.Length; i++)
                if (tokens[i].Equals("DESC", StringComparison.OrdinalIgnoreCase)) orderBy.Direction = SortDirection.DESC;
                else if (tokens[i].Equals("NULLS", StringComparison.OrdinalIgnoreCase) && i + 1 < tokens.Length) { orderBy.NullsOrder = tokens[i + 1].Equals("FIRST", StringComparison.OrdinalIgnoreCase) ? NullsOrder.First : NullsOrder.Last; i++; }
            res.Add(orderBy);
        }
        return res;
    }

    void ResolveRelationships(SoqlQuery s) {
        var metadata = _metadataProvider.GetObjectMetadata(s.FromObject);
        var relationships = new Dictionary<string, SoqlJoin>();
        // find all relationship fields in SELECT and WHERE
        foreach (var field in s.SelectFields.Where(f => f.FieldName?.Contains('.') == true)) {
            var parts = field.FieldName!.Split('.');
            var relationshipName = parts[0];
            if (!relationships.ContainsKey(relationshipName)) {
                var relationship = metadata.Relationships.FirstOrDefault(r => r.RelationshipName.Equals(relationshipName, StringComparison.OrdinalIgnoreCase));
                if (relationship != null) {
                    relationships[relationshipName] = new SoqlJoin {
                        RelationshipName = relationshipName,
                        TargetObject = relationship.TargetObject,
                        ForeignKey = relationship.ForeignKeyField,
                        PrimaryKey = relationship.ReferencedKeyField
                    };
                }
            }
        }
        s.Joins = [.. relationships.Values];
    }

    static int FindTopLevelOperator(string s, string op) {
        int level = 0, i = 0;
        while (i < s.Length) {
            if (s[i] == '(') level++;
            else if (s[i] == ')') level--;
            else if (level == 0 && s[i..].StartsWith(op, StringComparison.OrdinalIgnoreCase) && (i == 0 || char.IsWhiteSpace(s[i - 1])) && (i + op.Length >= s.Length || char.IsWhiteSpace(s[i + op.Length]))) return i;
            i++;
        }
        return -1;
    }

    static List<string> SplitRespectingParentheses(string s, char delimiter) {
        var res = new List<string>();
        var current = "";
        int level = 0;
        foreach (var c in s) {
            if (c == '(') level++;
            else if (c == ')') level--;
            if (c == delimiter && level == 0) { res.Add(current); current = ""; }
            else current += c;
        }
        if (!string.IsNullOrEmpty(current)) res.Add(current);
        return res;
    }

    [GeneratedRegex(@"SELECT\s+(.*?)\s+FROM", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")] private static partial Regex SELECT();
    [GeneratedRegex(@"FROM\s+(\w+)", RegexOptions.IgnoreCase, "en-US")] private static partial Regex FROM();
    [GeneratedRegex(@"WHERE\s+(.*?)(?=\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|\s+OFFSET|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")] private static partial Regex WHERE();
    [GeneratedRegex(@"ORDER\s+BY\s+(.*?)(?=\s+LIMIT|\s+OFFSET|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")] private static partial Regex ORDERBY();
    [GeneratedRegex(@"OFFSET\s+(\d+)", RegexOptions.IgnoreCase, "en-US")] private static partial Regex OFFSET();
    [GeneratedRegex(@"HAVING\s+(.*?)(?=\s+ORDER\s+BY|\s+LIMIT|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")] private static partial Regex HAVING();
    [GeneratedRegex(@"LIMIT\s+(\d+)", RegexOptions.IgnoreCase, "en-US")] private static partial Regex LIMIT();
    [GeneratedRegex(@"GROUP\s+BY\s+(.*?)(?=\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|$)", RegexOptions.IgnoreCase, "en-US")] private static partial Regex GROUPBY();
    [GeneratedRegex(@"(COUNT|SUM|AVG|MIN|MAX)\s*\((.*?)\)(?:\s+(\w+))?", RegexOptions.IgnoreCase, "en-US")] private static partial Regex AGGREGATE();
    [GeneratedRegex(@"(\w+(?:\.\w+)?)\s+(NOT\s+)?IN\s+\((.*?)\)", RegexOptions.IgnoreCase, "en-US")] private static partial Regex IN();
    [GeneratedRegex(@"(\w+(?:\.\w+)?)\s+IS\s+(NOT\s+)?NULL", RegexOptions.IgnoreCase, "en-US")] private static partial Regex IS_NULL();
    [GeneratedRegex(@"(\w+(?:\.\w+)?)\s+LIKE\s+(.+)", RegexOptions.IgnoreCase, "en-US")] private static partial Regex LIKE();
    [GeneratedRegex(@"(\w+(?:\.\w+)?)\s*([!=<>]+)\s*(.+)", RegexOptions.IgnoreCase, "en-US")] private static partial Regex OP();
}

#endregion