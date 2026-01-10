using Dapper;
using Open.Soql.Metadata;
using Open.Soql.Parser;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Text;

namespace Open.Soql.Query;

#region Generator

public class CompiledQuery {
    public required string Sql { get; set; }
    public required Dictionary<string, object> Parameters { get; set; }
    public required QueryPlan Plan { get; set; }
}

public class SoqlEngineOptions {
    public bool EnableRowLevelSecurity { get; set; } = true;
    public bool EnablePlanCaching { get; set; } = true;
    public bool EnableResultCaching { get; set; } = false;
    public int PlanCacheSize { get; set; } = 1000;
    public TimeSpan PlanCacheTtl { get; set; } = TimeSpan.FromHours(1);
    public int ResultCacheSize { get; set; } = 100;
    public TimeSpan ResultCacheTtl { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxResultCacheSize { get; set; } = 1000;
    public bool EnableParallelExecution { get; set; } = true;
    public int MaxParallelDegree { get; set; } = 4;
}

public class QueryExecutionContext {
    public required string Soql { get; set; }
    public Type? ResultType { get; set; }
    public QueryExecutionMetadata Metadata { get; set; } = new();
}

/// <summary>
/// Generates native SQL from SOQL queries for different database providers
/// </summary>
public class SqlGenerator(DatabaseProvider provider, IMetadataProvider metadataProvider) {
    readonly DatabaseProvider _provider = provider;
    readonly IMetadataProvider _metadataProvider = metadataProvider;
    int _parameterIndex = 0;

    public CompiledQuery GenerateSql(QueryPlan plan) {
        _parameterIndex = 0;
        var parameters = new Dictionary<string, object>();
        var sql = GenerateQuerySql(plan.Query, plan, parameters);
        return new CompiledQuery { Sql = sql, Parameters = parameters, Plan = plan };
    }

    string GenerateQuerySql(SoqlQuery query, QueryPlan plan, Dictionary<string, object> parameters) {
        var metadata = _metadataProvider.GetObjectMetadata(query.FromObject);
        var joins = plan.OptimizedJoinOrder.Count != 0 ? plan.OptimizedJoinOrder : query.Joins;
        var sql = new StringBuilder();
        sql.Append("SELECT "); sql.Append(GenerateSelectClause(query, metadata, plan)); // SELECT clause
        sql.Append($"\nFROM {GetTableName(metadata)} t0"); // FROM clause
        if (joins != null) for (var i = 0; i < joins.Count; i++) { sql.Append('\n'); sql.Append(GenerateJoinClause(joins[i], metadata, i + 1)); } // JOIN clauses (use optimized order if available)
        if (query.WhereClause != null) { sql.Append("\nWHERE "); sql.Append(GenerateConditionSql(query.WhereClause, "t0", parameters)); } // WHERE clause
        if (query.GroupBy != null && query.GroupBy.Count != 0) { sql.Append("\nGROUP BY "); sql.Append(string.Join(", ", query.GroupBy.Select(g => $"t0.{GetColumnName(metadata, g)}"))); } // GROUP BY clause
        if (query.HavingClause != null) { sql.Append("\nHAVING "); sql.Append(GenerateConditionSql(query.HavingClause, "t0", parameters)); } // HAVING clause
        if (query.OrderBy != null && query.OrderBy.Count != 0) { sql.Append("\nORDER BY "); sql.Append(GenerateOrderByClause(query.OrderBy, metadata)); } // ORDER BY clause
        if (query.Limit.HasValue || query.Offset.HasValue) sql.Append(GenerateLimitOffsetClause(query.Limit, query.Offset)); // LIMIT/OFFSET
        return sql.ToString();
    }

    string GenerateSelectClause(SoqlQuery query, ObjectMetadata metadata, QueryPlan plan) {
        var res = new List<string>();
        foreach (var field in query.SelectFields) {
            if (field.SubQuery != null) { var subQuerySql = $"({GenerateQuerySql(field.SubQuery, plan, [])})"; res.Add(field.Alias != null ? $"{subQuerySql} AS {field.Alias}" : subQuerySql); } // Subquery in SELECT (correlated)
            else if (field.IsAggregate) { var aggSql = GenerateAggregateExpression(field, "t0", metadata); res.Add(field.Alias != null ? $"{aggSql} AS {field.Alias}" : aggSql); }
            else if (field.FieldName!.Contains('.') && query.Joins != null) {
                var parts = field.FieldName.Split('.'); // Relationship field
                var joinIndex = query.Joins.FindIndex(j => j.RelationshipName.Equals(parts[0], StringComparison.OrdinalIgnoreCase));
                if (joinIndex >= 0) {
                    var joinAlias = $"t{joinIndex + 1}";
                    var targetMetadata = _metadataProvider.GetObjectMetadata(query.Joins[joinIndex].TargetObject);
                    var columnName = GetColumnName(targetMetadata, parts[1]);
                    res.Add(field.Alias != null ? $"{joinAlias}.{columnName} as {field.Alias}" : $"{joinAlias}.{columnName}");
                }
            }
            else { var columnName = GetColumnName(metadata, field.FieldName); res.Add(field.Alias != null ? $"t0.{columnName} as {field.Alias}" : $"t0.{columnName}"); }
        }
        return string.Join(", ", res);
    }

    string GenerateAggregateExpression(SoqlField field, string tableAlias, ObjectMetadata metadata) {
        var aggFunc = field.AggregateType!.Value;
        var fieldExpr = field.AggregateField != null ? $"{tableAlias}.{GetColumnName(metadata, field.AggregateField)}" : "*";
        return aggFunc switch {
            AggregateFunction.COUNT => $"COUNT({fieldExpr})",
            AggregateFunction.COUNT_DISTINCT => $"COUNT(DISTINCT {fieldExpr})",
            AggregateFunction.SUM => $"SUM({fieldExpr})",
            AggregateFunction.AVG => $"AVG({fieldExpr})",
            AggregateFunction.MIN => $"MIN({fieldExpr})",
            AggregateFunction.MAX => $"MAX({fieldExpr})",
            _ => throw new NotSupportedException($"Aggregate function {aggFunc} not supported")
        };
    }

    string GenerateJoinClause(SoqlJoin join, ObjectMetadata baseMetadata, int joinIndex) {
        var joinType = join.Type switch {
            JoinType.INNER => "INNER JOIN",
            JoinType.LEFT => "LEFT JOIN",
            JoinType.RIGHT => "RIGHT JOIN",
            _ => "LEFT JOIN"
        };
        var targetMetadata = _metadataProvider.GetObjectMetadata(join.TargetObject);
        var targetTable = GetTableName(targetMetadata);
        var alias = $"t{joinIndex}";
        var foreignKeyColumn = GetColumnName(baseMetadata, join.ForeignKey!);
        var primaryKeyColumn = GetColumnName(targetMetadata, join.PrimaryKey!);
        return $"{joinType} {targetTable} {alias} On t0.{foreignKeyColumn} = {alias}.{primaryKeyColumn}";
    }

    string GenerateConditionSql(SoqlCondition condition, string tableAlias, Dictionary<string, object> parameters) {
        if (condition.IsCompound) {
            var left = GenerateConditionSql(condition.Left!, tableAlias, parameters);
            var right = GenerateConditionSql(condition.Right!, tableAlias, parameters);
            var op = condition.LogicalOp == LogicalOperator.AND ? "AND" : "OR";
            return $"({left} {op} {right})";
        }
        var fieldName = GetFieldExpression(condition.Field!, tableAlias);
        switch (condition.Operator) {
            case SoqlOperator.Equals: return $"{fieldName} = {AddParameter(parameters, condition.Value!)}";
            case SoqlOperator.NotEquals: return $"{fieldName} <> {AddParameter(parameters, condition.Value!)}";
            case SoqlOperator.LessThan: return $"{fieldName} < {AddParameter(parameters, condition.Value!)}";
            case SoqlOperator.LessThanOrEqual: return $"{fieldName} <= {AddParameter(parameters, condition.Value!)}";
            case SoqlOperator.GreaterThan: return $"{fieldName} > {AddParameter(parameters, condition.Value!)}";
            case SoqlOperator.GreaterThanOrEqual: return $"{fieldName} >= {AddParameter(parameters, condition.Value!)}";
            case SoqlOperator.Like or SoqlOperator.Contains or SoqlOperator.StartsWith or SoqlOperator.EndsWith: return GenerateLikeExpression(fieldName, condition.Operator, condition.Value!, parameters);
            case SoqlOperator.In:
                if (condition.SubQuery != null) return $"{fieldName} In ({GenerateQuerySql(condition.SubQuery, new QueryPlan { Query = condition.SubQuery }, parameters)})";
                else if (condition.Value is List<object> list) return $"{fieldName} In ({string.Join(", ", list.Select(v => AddParameter(parameters, v)))})";
                break;
            case SoqlOperator.NotIn:
                if (condition.SubQuery != null) return $"{fieldName} Not In ({GenerateQuerySql(condition.SubQuery, new QueryPlan { Query = condition.SubQuery }, parameters)})";
                else if (condition.Value is List<object> list) return $"{fieldName} Not In ({string.Join(", ", list.Select(v => AddParameter(parameters, v)))})";
                break;
            case SoqlOperator.IsNull: return $"{fieldName} IS NULL";
            case SoqlOperator.IsNotNull: return $"{fieldName} IS NOT NULL";
        }
        throw new NotSupportedException($"Operator {condition.Operator} not supported");
    }

    string GenerateLikeExpression(string fieldName, SoqlOperator op, object value, Dictionary<string, object> parameters) {
        var pattern = value?.ToString() ?? "";
        pattern = op switch {
            SoqlOperator.Contains => $"%{pattern.Trim('%')}%",
            SoqlOperator.StartsWith => $"{pattern.TrimEnd('%')}%",
            SoqlOperator.EndsWith => $"%{pattern.TrimStart('%')}",
            _ => pattern
        };
        var paramName = AddParameter(parameters, pattern);
        // Postgres uses ILIKE for case-insensitive matching
        var likeOp = _provider == DatabaseProvider.Postgres ? "ILIKE" : "LIKE";
        return $"{fieldName} {likeOp} {paramName}";
    }

    string GenerateOrderByClause(List<SoqlOrderBy> orderByList, ObjectMetadata metadata) {
        var items = orderByList.Select(o => {
            var columnName = GetColumnName(metadata, o.Field);
            var direction = o.Direction == SortDirection.DESC ? "DESC" : "ASC";
            var nulls = o.NullsOrder == NullsOrder.First ? "NULLS FIRST" : "NULLS LAST";
            // SQL Server doesn't support NULLS FIRST/LAST directly
            if (_provider == DatabaseProvider.SqlServer) {
                var nullSort = o.NullsOrder == NullsOrder.First ? "0" : "1";
                return $"CASE WHEN t0.{columnName} IS NULL THEN {nullSort} ELSE 2 END, t0.{columnName} {direction}";
            }
            return $"t0.{columnName} {direction} {nulls}";
        });
        return string.Join(", ", items);
    }

    string GenerateLimitOffsetClause(int? limit, int? offset) => _provider switch {
        DatabaseProvider.Postgres => GeneratePostgresLimitOffset(limit, offset),
        DatabaseProvider.SqlServer => GenerateSqlServerOffsetFetch(limit, offset),
        _ => ""
    };

    static string GeneratePostgresLimitOffset(int? limit, int? offset) {
        var b = new StringBuilder();
        if (limit.HasValue) b.Append($"\nLIMIT {limit.Value}");
        if (offset.HasValue) b.Append($"\nOFFSET {offset.Value}");
        return b.ToString();
    }

    static string GenerateSqlServerOffsetFetch(int? limit, int? offset) {
        // SQL Server requires ORDER BY for OFFSET/FETCH
        var b = new StringBuilder();
        if (offset.HasValue) { b.Append($"\nOFFSET {offset.Value} ROWS"); if (limit.HasValue) b.Append($"\nFETCH NEXT {limit.Value} ROWS ONLY"); }
        else if (limit.HasValue) b.Append($"\nOFFSET 0 ROWS\nFETCH NEXT {limit.Value} ROWS ONLY"); // Use TOP in SELECT clause instead (would need to modify SELECT generation)
        return b.ToString();
    }

    static string GetFieldExpression(string fieldName, string tableAlias) =>
        // Handle relationship traversal (e.g., Account.Name)
        fieldName.Contains('.') ? fieldName.Replace('.', '_') // This should have been resolved to a join already
        : $"{tableAlias}.{fieldName}";

    string GetTableName(ObjectMetadata metadata) => _provider switch {
        DatabaseProvider.Postgres => $"\"{metadata.TableName}\"",
        DatabaseProvider.SqlServer => $"[{metadata.TableName}]",
        _ => metadata.TableName!
    };

    string GetColumnName(ObjectMetadata metadata, string fieldName) {
        var field = metadata.Fields.GetValueOrDefault(fieldName);
        var columnName = field?.ColumnName ?? fieldName;
        return _provider switch {
            DatabaseProvider.Postgres => $"\"{columnName}\"",
            DatabaseProvider.SqlServer => $"[{columnName}]",
            _ => columnName
        };
    }

    string AddParameter(Dictionary<string, object> parameters, object value) {
        var paramName = _provider switch {
            DatabaseProvider.Postgres => $"@p{_parameterIndex}",
            DatabaseProvider.SqlServer => $"@p{_parameterIndex}",
            _ => $"@p{_parameterIndex}"
        };
        parameters[paramName] = value;
        _parameterIndex++;
        return paramName;
    }
}

#endregion

#region Executor

/// <summary>
/// Executes compiled SOQL queries with parallel relationship loading and result mapping
/// </summary>
public class QueryExecutor(IDbConnection connection, DatabaseProvider provider, IMetadataProvider metadataProvider) {
    readonly IDbConnection _connection = connection;
    readonly DatabaseProvider _provider = provider;
    readonly IMetadataProvider _metadataProvider = metadataProvider;
    readonly ExpressionTreeCompiler _expressionCompiler = new(metadataProvider);

    /// <summary>
    /// Execute query and return strongly-typed results
    /// </summary>
    public async Task<List<T>> ExecuteQueryAsync<T>(CompiledQuery compiledQuery) where T : new() {
        var results = await ExecuteQueryAsync(compiledQuery, typeof(T));
        return [.. results.Cast<T>()];
    }

    /// <summary>
    /// Execute query and return dynamic results
    /// </summary>
    public async Task<List<object>> ExecuteQueryAsync(CompiledQuery compiledQuery, Type? resultType = null) {
        var plan = compiledQuery.Plan;
        var query = plan.Query;
        var rawResults = await ExecuteMainQueryAsync(compiledQuery); // Execute main query
        if (query.Joins != null && query.Joins.Count != 0 && plan.UseParallelExecution) await LoadRelationshipsInParallelAsync(rawResults, query); // Parallel relationship loading if there are joins
        if (resultType != null) { var mapper = _expressionCompiler.CompileMapper(query, resultType); return [.. rawResults.Select(mapper)]; } // Map to result type using compiled expressions
        return rawResults;
    }

    async Task<List<object>> ExecuteMainQueryAsync(CompiledQuery compiledQuery) {
        var sql = compiledQuery.Sql;
        var parameters = new DynamicParameters(compiledQuery.Parameters);
        var results = await _connection.QueryAsync<dynamic>(sql, parameters); // Execute with Dapper
        return [.. results.Cast<object>()];
    }

    async Task LoadRelationshipsInParallelAsync(List<object> results, SoqlQuery query) {
        if (results.Count == 0) return;
        var tasks = new List<Task>();
        foreach (var join in query.Joins!) tasks.Add(Task.Run(async () => await LoadRelationshipDataAsync(results, join, query.FromObject)));
        await Task.WhenAll(tasks);
    }

    async Task LoadRelationshipDataAsync(List<object> results, SoqlJoin join, string baseObject) {
        var baseMetadata = _metadataProvider.GetObjectMetadata(baseObject);
        var targetMetadata = _metadataProvider.GetObjectMetadata(join.TargetObject);
        var foreignKeyValues = results.Select(r => GetPropertyValue(r, join.ForeignKey!)).Where(v => v != null).Distinct().ToList(); // extract foreign key values
        if (foreignKeyValues.Count == 0) return;
        var relatedRecords = await _connection.QueryAsync<dynamic>(BuildLookupQuery(targetMetadata, join.PrimaryKey!, foreignKeyValues!), new { ids = foreignKeyValues }); // build and execute lookup query
        var relatedLookup = relatedRecords.GroupBy(r => GetPropertyValue(r, join.PrimaryKey)).ToDictionary(g => g.Key, g => g.First()); // create lookup dictionary
        // attach related records to results
        foreach (var result in results) {
            var foreignKeyValue = GetPropertyValue(result, join.ForeignKey!);
            if (foreignKeyValue != null && relatedLookup.TryGetValue(foreignKeyValue, out var relatedRecord))
                SetPropertyValue(result, join.RelationshipName, relatedRecord);
        }
    }

    string BuildLookupQuery(ObjectMetadata metadata, string primaryKey, List<object> values) {
        var tableName = GetTableName(metadata);
        var columnName = GetColumnName(metadata, primaryKey);
        return _provider switch {
            DatabaseProvider.Postgres => $"SELECT * FROM {tableName} WHERE {columnName} = ANY(@ids)",
            DatabaseProvider.SqlServer => $"SELECT * FROM {tableName} WHERE {columnName} IN @ids",
            _ => $"SELECT * FROM {tableName} WHERE {columnName} IN @ids"
        };
    }

    static object? GetPropertyValue(object obj, string propertyName) {
        if (obj is IDictionary<string, object> dict) return dict.TryGetValue(propertyName, out var value) ? value : null;
        var property = obj.GetType().GetProperty(propertyName);
        return property?.GetValue(obj);
    }

    static void SetPropertyValue(object obj, string propertyName, object value) {
        if (obj is IDictionary<string, object> dict) { dict[propertyName] = value; return; }
        var property = obj.GetType().GetProperty(propertyName);
        property?.SetValue(obj, value);
    }

    string GetTableName(ObjectMetadata metadata) => _provider switch {
        DatabaseProvider.Postgres => $"\"{metadata.TableName}\"",
        DatabaseProvider.SqlServer => $"[{metadata.TableName}]",
        _ => metadata.TableName!
    };

    string GetColumnName(ObjectMetadata metadata, string fieldName) {
        var field = metadata.Fields.GetValueOrDefault(fieldName);
        var columnName = field?.ColumnName ?? fieldName;
        return _provider switch {
            DatabaseProvider.Postgres => $"\"{columnName}\"",
            DatabaseProvider.SqlServer => $"[{columnName}]",
            _ => columnName
        };
    }
}

/// <summary>
/// Compiles LINQ expression trees for high-performance result mapping
/// </summary>
public class ExpressionTreeCompiler(IMetadataProvider metadataProvider) {
    readonly IMetadataProvider _metadataProvider = metadataProvider;
    readonly Dictionary<string, Delegate> _compiledMappers = [];

    /// <summary>
    /// Compile a mapper function from dynamic result to strongly-typed object
    /// </summary>
    public Func<object, object> CompileMapper(SoqlQuery query, Type targetType) {
        var cacheKey = $"{query.FromObject}:{targetType.FullName}:{GetFieldsHash(query)}";
        if (_compiledMappers.TryGetValue(cacheKey, out var cached)) return (Func<object, object>)cached;
        var mapper = BuildMapper(query, targetType);
        _compiledMappers[cacheKey] = mapper;
        return mapper;
    }

    Func<object, object> BuildMapper(SoqlQuery query, Type targetType) {
        var sourceParam = Expression.Parameter(typeof(object), "source");
        var sourceDict = Expression.Variable(typeof(IDictionary<string, object>), "dict");

        // cast source to dictionary
        var castToDict = Expression.Assign(sourceDict, Expression.TypeAs(sourceParam, typeof(IDictionary<string, object>)));

        // create target instance
        var targetVar = Expression.Variable(targetType, "target");
        var createTarget = Expression.Assign(targetVar, Expression.New(targetType));
        var assignments = new List<Expression> { castToDict, createTarget };

        // map each field
        //var metadata = _metadataProvider.GetObjectMetadata(query.FromObject); //TODO (not used)
        foreach (var field in query.SelectFields) {
            var targetProperty = targetType.GetProperty(field.Alias ?? field.FieldName!);
            if (targetProperty == null || !targetProperty.CanWrite) continue;
            var fieldName = field.Alias ?? field.FieldName;
            // dict.TryGetValue(fieldName, out var value)
            var valueVar = Expression.Variable(typeof(object), $"value_{fieldName}");
            var tryGetValue = Expression.Call(sourceDict, typeof(IDictionary<string, object>).GetMethod("TryGetValue")!, Expression.Constant(fieldName), valueVar);
            // convert and assign if value exists
            var convertedValue = Expression.Convert(valueVar, targetProperty.PropertyType);
            var assignProperty = Expression.Assign(Expression.Property(targetVar, targetProperty), convertedValue);
            var conditionalAssign = Expression.IfThen(tryGetValue, assignProperty);
            assignments.Add(Expression.Block([valueVar], conditionalAssign));
        }

        // return target
        var returnTarget = Expression.Convert(targetVar, typeof(object));
        assignments.Add(returnTarget);

        // build
        var body = Expression.Block([sourceDict, targetVar], assignments);
        var lambda = Expression.Lambda<Func<object, object>>(body, sourceParam);
        return lambda.Compile();
    }

    /// <summary>
    /// Compile a filter predicate for in-memory filtering
    /// </summary>
    public Func<T, bool> CompileFilter<T>(SoqlCondition condition) {
        var param = Expression.Parameter(typeof(T), "record");
        var body = BuildConditionExpression(condition, param);
        var lambda = Expression.Lambda<Func<T, bool>>(body, param);
        return lambda.Compile();
    }

    static BinaryExpression BuildConditionExpression(SoqlCondition condition, ParameterExpression param) {
        if (condition.IsCompound) {
            var left = BuildConditionExpression(condition.Left!, param);
            var right = BuildConditionExpression(condition.Right!, param);
            return condition.LogicalOp == LogicalOperator.AND ? Expression.AndAlso(left, right) : Expression.OrElse(left, right);
        }
        var property = Expression.Property(param, condition.Field!);
        var constantValue = Expression.Constant(condition.Value);
        return condition.Operator switch {
            SoqlOperator.Equals => Expression.Equal(property, constantValue),
            SoqlOperator.NotEquals => Expression.NotEqual(property, constantValue),
            SoqlOperator.LessThan => Expression.LessThan(property, constantValue),
            SoqlOperator.LessThanOrEqual => Expression.LessThanOrEqual(property, constantValue),
            SoqlOperator.GreaterThan => Expression.GreaterThan(property, constantValue),
            SoqlOperator.GreaterThanOrEqual => Expression.GreaterThanOrEqual(property, constantValue),
            SoqlOperator.IsNull => Expression.Equal(property, Expression.Constant(null)),
            SoqlOperator.IsNotNull => Expression.NotEqual(property, Expression.Constant(null)),
            _ => throw new NotSupportedException($"Operator {condition.Operator} not supported in expressions")
        };
    }

    static string GetFieldsHash(SoqlQuery query) => string.Join(",", query.SelectFields.Select(f => f.FieldName)).GetHashCode().ToString("X");
}

/// <summary>
/// Streaming query executor for large result sets
/// </summary>
public class StreamingQueryExecutor(IDbConnection connection, DatabaseProvider _) {
    readonly IDbConnection _connection = connection;

    /// <summary>
    /// Execute query and stream results using IAsyncEnumerable
    /// </summary>
    public async IAsyncEnumerable<T> ExecuteStreamingAsync<T>(CompiledQuery compiledQuery) {
        var parameters = new DynamicParameters(compiledQuery.Parameters);
        // use Dapper's buffered: false for streaming
        using var r = (DbDataReader)await _connection.ExecuteReaderAsync(new CommandDefinition(compiledQuery.Sql, parameters), CommandBehavior.SequentialAccess);
        var parser = r.GetRowParser<T>();
        while (await r.ReadAsync()) yield return parser(r);
    }
}

/// <summary>
/// Batch query executor for processing multiple queries efficiently
/// </summary>
public class BatchQueryExecutor(IDbConnection connection, QueryExecutor _) {
    readonly IDbConnection _connection = connection;

    /// <summary>
    /// Execute multiple queries in a single database round-trip
    /// </summary>
    public async Task<Dictionary<string, List<object>>> ExecuteBatchAsync(Dictionary<string, CompiledQuery> queries) {
        // combine all queries into a single batch
        var batchSql = string.Join(";\n", queries.Select(q => q.Value.Sql));
        var allParameters = new DynamicParameters();
        foreach (var query in queries)
            foreach (var param in query.Value.Parameters)
                allParameters.Add($"{query.Key}_{param.Key}", param.Value);
        // execute batch
        using var multi = await _connection.QueryMultipleAsync(batchSql, allParameters);
        var res = new Dictionary<string, List<object>>();
        foreach (var query in queries) { var queryResults = await multi.ReadAsync<dynamic>(); res[query.Key] = [.. queryResults.Cast<object>()]; }
        return res;
    }
}

#endregion

#region Rls Provider

public class RlsPolicy {
    public required string Name { get; set; }
    public string? Description { get; set; }
    public RlsPolicyType PolicyType { get; set; }
    public Func<SecurityContext, SoqlCondition>? Condition { get; set; }
    public Func<SecurityContext, bool> IsApplicable { get; set; } = _ => true;
}

public enum RlsPolicyType { OwnerBased, SharingBased, HierarchyBased, TerritoryBased, Custom }

public enum AccessType { Read, Create, Update, Delete }

/// <summary>
/// Enforces row-level security (RLS) policies on queries
/// Supports user-based, role-based, and hierarchical security
/// </summary>
public class RlsEnforcer(ISecurityContextProvider securityContextProvider, IMetadataProvider metadataProvider) {
    readonly ISecurityContextProvider _securityContextProvider = securityContextProvider;
    readonly IMetadataProvider _metadataProvider = metadataProvider;
    readonly Dictionary<string, RlsPolicy> _policies = [];

    /// <summary>
    /// Apply RLS to a query based on the current security context
    /// </summary>
    public SoqlQuery ApplyRowLevelSecurity(SoqlQuery query) {
        var context = _securityContextProvider.GetCurrentContext();
        var metadata = _metadataProvider.GetObjectMetadata(query.FromObject);
        if (!metadata.HasRls) return query; // No RLS for this object

        // Get applicable policies
        var policies = GetApplicablePolicies(query.FromObject, context);
        if (policies.Count == 0) return query; // User has bypass permission or no policies apply

        // Build RLS conditions
        var rlsCondition = BuildRlsCondition(policies, context, query.FromObject);
        if (rlsCondition == null) return query;

        // Merge with existing WHERE clause
        query.WhereClause = query.WhereClause != null ? new SoqlCondition { LogicalOp = LogicalOperator.AND, Left = query.WhereClause, Right = rlsCondition } : rlsCondition;
        return query;
    }

    /// <summary>
    /// Register a custom RLS policy
    /// </summary>
    public void RegisterPolicy(string objectName, RlsPolicy policy) => _policies[$"{objectName}:{policy.Name}"] = policy;

    public RlsEnforcer UseDefaultPolicies() {
        // Owner-based policy
        RegisterPolicy("*", new RlsPolicy {
            Name = "OwnerOnly",
            Description = "Users can only see records they own",
            PolicyType = RlsPolicyType.OwnerBased,
            Condition = ctx => new SoqlCondition { Field = "OwnerId", Operator = SoqlOperator.Equals, Value = ctx.UserId }
        });
        // Sharing-based policy
        RegisterPolicy("*", new RlsPolicy {
            Name = "SharingRules",
            Description = "Users can see records shared with them",
            PolicyType = RlsPolicyType.SharingBased,
            Condition = ctx => new SoqlCondition {
                LogicalOp = LogicalOperator.OR,
                Left = new SoqlCondition { Field = "OwnerId", Operator = SoqlOperator.Equals, Value = ctx.UserId },
                Right = new SoqlCondition {
                    Field = "Id",
                    Operator = SoqlOperator.In,
                    SubQuery = new SoqlQuery {
                        FromObject = "Share",
                        SelectFields = [new SoqlField { FieldName = "RecordId" }],
                        WhereClause = new SoqlCondition { Field = "UserOrGroupId", Operator = SoqlOperator.Equals, Value = ctx.UserId }
                    }
                }
            }
        });
        // Role hierarchy policy
        RegisterPolicy("*", new RlsPolicy {
            Name = "RoleHierarchy",
            Description = "Users can see records owned by subordinates",
            PolicyType = RlsPolicyType.HierarchyBased,
            Condition = ctx => new SoqlCondition {
                Field = "OwnerId",
                Operator = SoqlOperator.In,
                SubQuery = new SoqlQuery {
                    FromObject = "UserRoleHierarchy",
                    SelectFields = [new SoqlField { FieldName = "SubordinateUserId" }],
                    WhereClause = new SoqlCondition { Field = "SupervisorUserId", Operator = SoqlOperator.Equals, Value = ctx.UserId }
                }
            }
        });
        // Territory-based policy
        RegisterPolicy("*", new RlsPolicy {
            Name = "TerritoryBased",
            Description = "Users can see records in their territory",
            PolicyType = RlsPolicyType.TerritoryBased,
            Condition = ctx => new SoqlCondition { Field = "TerritoryId", Operator = SoqlOperator.In, Value = ctx.TerritoryIds }
        });
        return this;
    }

    List<RlsPolicy> GetApplicablePolicies(string objectName, SecurityContext context) {
        // system admins bypass RLS
        if (context.Roles.Contains("SystemAdministrator")) return [];
        var policies = new List<RlsPolicy>();
        // get object-specific policies
        policies.AddRange(_policies.Where(p => p.Key.StartsWith($"{objectName}:")).Select(p => p.Value));
        // get wildcard policies if no specific ones exist
        if (policies.Count == 0) policies.AddRange(_policies.Where(p => p.Key.StartsWith("*:")).Select(p => p.Value).Where(p => p.IsApplicable(context)));
        return policies;
    }

    static SoqlCondition? BuildRlsCondition(List<RlsPolicy> policies, SecurityContext context, string objectName) {
        if (policies.Count == 0) return null;
        SoqlCondition? combinedCondition = null;
        foreach (var policy in policies) {
            var policyCondition = policy.Condition!(context);
            // combine with OR (user has access if ANY policy grants it)
            combinedCondition = combinedCondition == null ? policyCondition : new SoqlCondition { LogicalOp = LogicalOperator.OR, Left = combinedCondition, Right = policyCondition };
        }
        return combinedCondition;
    }

    /// <summary>
    /// Validate that a record satisfies RLS policies (for DML operations)
    /// </summary>
    public bool ValidateRecordAccess(string objectName, Dictionary<string, object> record, AccessType accessType) {
        var context = _securityContextProvider.GetCurrentContext();
        var metadata = _metadataProvider.GetObjectMetadata(objectName);
        if (!metadata.HasRls) return true;
        var policies = GetApplicablePolicies(objectName, context);
        if (policies.Count == 0) return true;
        // check if record satisfies any policy
        foreach (var policy in policies)
            if (EvaluateConditionAgainstRecord(policy.Condition!(context), record)) return true;
        return false;
    }

    static bool EvaluateConditionAgainstRecord(SoqlCondition condition, Dictionary<string, object> record) {
        if (condition.IsCompound) {
            var leftResult = EvaluateConditionAgainstRecord(condition.Left!, record);
            var rightResult = EvaluateConditionAgainstRecord(condition.Right!, record);
            return condition.LogicalOp == LogicalOperator.AND ? leftResult && rightResult : leftResult || rightResult;
        }
        if (!record.TryGetValue(condition.Field!, out var fieldValue)) return false;
        return condition.Operator switch {
            SoqlOperator.Equals => Equals(fieldValue, condition.Value),
            SoqlOperator.NotEquals => !Equals(fieldValue, condition.Value),
            SoqlOperator.In => condition.Value is IEnumerable<object> list && list.Contains(fieldValue),
            SoqlOperator.IsNull => fieldValue == null,
            SoqlOperator.IsNotNull => fieldValue != null,
            _ => false
        };
    }
}

#endregion

#region Engine

public class QueryExecutionMetadata {
    public SoqlQuery? ParsedQuery { get; set; }
    public QueryPlan? OptimizedPlan { get; set; }
    public string? CompiledSql { get; set; }
    public bool UsedCachedPlan { get; set; }
    public bool UsedCachedResults { get; set; }
}

public class QueryResult<T> {
    public List<T>? Records { get; set; }
    public bool Success { get; set; }
    public string? Error { get; set; }
    public TimeSpan ExecutionTime { get; set; }
    public int RecordCount { get; set; }
    public QueryExecutionMetadata? Metadata { get; set; }
}

/// <summary>
/// Main SOQL execution engine that orchestrates parsing, optimization, compilation, and execution
/// </summary>
public class SoqlEngine {
    readonly DatabaseProvider _provider;
    readonly IDbConnection _connection;
    readonly IMetadataProvider _metadataProvider;
    readonly IStatisticsProvider _statisticsProvider;
    readonly ISecurityContextProvider _securityContextProvider;
    // components
    readonly SoqlParser _parser;
    readonly QueryOptimizer _optimizer;
    readonly SqlGenerator _sqlGenerator;
    readonly QueryExecutor _executor;
    readonly RlsEnforcer _rlsEnforcer;
    readonly QueryPlanCache _planCache;
    readonly ResultSetCache _resultCache;
    readonly ExpressionTreeCompiler _expressionCompiler;

    public SoqlEngineOptions Options { get; }

    public SoqlEngine(DatabaseProvider provider, IDbConnection connection, IMetadataProvider metadataProvider, IStatisticsProvider statisticsProvider, ISecurityContextProvider securityContextProvider, SoqlEngineOptions? options = null) {
        _provider = provider;
        _connection = connection;
        _metadataProvider = metadataProvider;
        _statisticsProvider = statisticsProvider;
        _securityContextProvider = securityContextProvider;
        Options = options ?? new SoqlEngineOptions();
        // initialize components
        _parser = new SoqlParser(_metadataProvider);
        _optimizer = new QueryOptimizer(_metadataProvider, _statisticsProvider);
        _sqlGenerator = new SqlGenerator(_provider, _metadataProvider);
        _executor = new QueryExecutor(_connection, _provider, _metadataProvider);
        _rlsEnforcer = new RlsEnforcer(_securityContextProvider, _metadataProvider);
        _planCache = new QueryPlanCache(Options.PlanCacheSize, Options.PlanCacheTtl);
        _resultCache = new ResultSetCache(Options.ResultCacheSize, Options.ResultCacheTtl);
        _expressionCompiler = new ExpressionTreeCompiler(_metadataProvider);
    }

    /// <summary>
    /// Execute a SOQL query and return strongly-typed results
    /// </summary>
    public async Task<List<T>> QueryAsync<T>(string soql) where T : new() => await ExecuteQueryAsync<T>(new QueryExecutionContext { Soql = soql, ResultType = typeof(T) });

    /// <summary>
    /// Execute a SOQL query and return dynamic results
    /// </summary>
    public async Task<List<object>> QueryAsync(string soql) => await ExecuteQueryAsync<object>(new QueryExecutionContext { Soql = soql });

    /// <summary>
    /// Execute a SOQL query with full context control
    /// </summary>
    public async Task<QueryResult<T>> ExecuteAsync<T>(string soql) where T : new() {
        var context = new QueryExecutionContext { Soql = soql, ResultType = typeof(T) };
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try {
            var results = await ExecuteQueryAsync<T>(context);
            sw.Stop();
            return new QueryResult<T> { Records = results, Success = true, ExecutionTime = sw.Elapsed, RecordCount = results.Count, Metadata = context.Metadata };
        }
        catch (Exception ex) {
            sw.Stop();
            return new QueryResult<T> { Success = false, Error = ex.Message, ExecutionTime = sw.Elapsed };
        }
    }

    async Task<List<T>> ExecuteQueryAsync<T>(QueryExecutionContext context) where T : new() {
        // 1. Parse SOQL
        var query = _parser.Parse(context.Soql);
        context.Metadata.ParsedQuery = query;
        // 2. Apply Row-Level Security
        if (Options.EnableRowLevelSecurity) query = _rlsEnforcer.ApplyRowLevelSecurity(query);
        // 3. Check plan cache
        var queryHash = GenerateQueryHash(query);
        var cachedPlan = _planCache.Get(queryHash);
        QueryPlan plan;
        if (cachedPlan != null && Options.EnablePlanCaching) { plan = cachedPlan; context.Metadata.UsedCachedPlan = true; }
        else {
            // 4. Optimize query plan
            plan = _optimizer.Optimize(query);
            context.Metadata.OptimizedPlan = plan;
            if (Options.EnablePlanCaching) await _planCache.SetAsync(queryHash, plan); // Cache the plan
        }
        // 5. Check result cache
        if (Options.EnableResultCaching) {
            var cachedResults = _resultCache.Get(queryHash);
            if (cachedResults != null) { context.Metadata.UsedCachedResults = true; return [.. cachedResults.Cast<T>()]; }
        }
        // 6. Compile to SQL
        var compiledQuery = _sqlGenerator.GenerateSql(plan);
        context.Metadata.CompiledSql = compiledQuery.Sql;
        // 7. Execute query
        var res = context.ResultType != null && context.ResultType != typeof(object)
            ? [.. (await _executor.ExecuteQueryAsync<T>(compiledQuery)).Cast<object>()]
            : await _executor.ExecuteQueryAsync(compiledQuery);
        // 8. Cache results
        if (Options.EnableResultCaching && res.Count <= Options.MaxResultCacheSize) _resultCache.Set(queryHash, res);
        return [.. res.Cast<T>()];
    }

    /// <summary>
    /// Get query execution plan without executing
    /// </summary>
    public async Task<QueryPlan> ExplainAsync(string soql) {
        var query = _parser.Parse(soql);
        if (Options.EnableRowLevelSecurity) query = _rlsEnforcer.ApplyRowLevelSecurity(query);
        var plan = _optimizer.Optimize(query);
        var compiledQuery = _sqlGenerator.GenerateSql(plan); // TODO (notused)
        // return plan with SQL
        plan.Query = query;
        return plan;
    }

    /// <summary>
    /// Invalidate all caches for a specific object
    /// </summary>
    public void InvalidateCache(string objectName) => _resultCache.InvalidateByObject(objectName);

    /// <summary>
    /// Clear all caches
    /// </summary>
    public async Task ClearCachesAsync() { await _planCache.ClearAsync(); _resultCache.Clear(); }

    /// <summary>
    /// Get cache statistics
    /// </summary>
    public CacheStatistics GetCacheStatistics() => _planCache.GetStatistics();

    string GenerateQueryHash(SoqlQuery query) {
        var components = new List<string> {
            query.FromObject,
            string.Join(",", query.SelectFields.Select(f => f.FieldName ?? f.SubQuery?.FromObject)),
            query.WhereClause != null ? SerializeCondition(query.WhereClause) : "",
            query.OrderBy != null ? string.Join(",", query.OrderBy.Select(o => $"{o.Field}:{o.Direction}")) : "",
            query.GroupBy != null ? string.Join(",", query.GroupBy) : "",
            query.HavingClause != null ? SerializeCondition(query.HavingClause) : "",
            query.Limit?.ToString() ?? "",
            query.Offset?.ToString() ?? ""
        };
        // Include security context in hash if RLS is enabled
        if (Options.EnableRowLevelSecurity) { var context = _securityContextProvider.GetCurrentContext(); components.Add(context.UserId); components.Add(string.Join(",", context.Roles)); }
        return ComputeHash(string.Join("|", components));
    }

    static string SerializeCondition(SoqlCondition condition)
        => condition == null ? ""
        : condition.IsCompound ? $"({SerializeCondition(condition.Left!)} {condition.LogicalOp} {SerializeCondition(condition.Right!)})"
        : $"{condition.Field} {condition.Operator} {condition.Value}";

    static string ComputeHash(string input) {
        using var sha = System.Security.Cryptography.SHA256.Create();
        return Convert.ToBase64String(sha.ComputeHash(Encoding.UTF8.GetBytes(input)));
    }
}

#endregion

#region Optimizer

public class QueryPlan {
    public string PlanId { get; set; }
    public SoqlQuery Query { get; set; }
    public int BaseTableCardinality { get; set; }
    public int FilteredCardinality { get; set; }
    public List<SoqlJoin> OptimizedJoinOrder { get; set; } = new();
    public List<IndexCandidate> SelectedIndexes { get; set; } = new();
    public bool UseParallelExecution { get; set; }
    public int ParallelDegree { get; set; } = 1;
    public bool UseHashAggregation { get; set; }
    public bool UseStreaming { get; set; }
    public double EstimatedCost { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

public class IndexCandidate {
    public string Field { get; set; }
    public SoqlOperator Operator { get; set; }
    public double Score { get; set; }
}

/// <summary>
/// Optimizes query execution plans using cost-based analysis
/// Handles join reordering, index selection, and execution strategy
/// </summary>
public class QueryOptimizer(IMetadataProvider metadataProvider, IStatisticsProvider statisticsProvider) {
    readonly IMetadataProvider _metadataProvider = metadataProvider;
    readonly IStatisticsProvider _statisticsProvider = statisticsProvider;

    class JoinOrderState {
        public double Cost { get; set; }
        public required List<SoqlJoin> Order { get; set; }
        public int Cardinality { get; set; }
    }

    public QueryPlan Optimize(SoqlQuery query) {
        var plan = new QueryPlan { Query = query, PlanId = GeneratePlanHash(query) };
        EstimateCardinalities(plan); // Estimate cardinalities
        if (query.Joins != null && query.Joins.Count != 0) OptimizeJoinOrder(plan); // Optimize join order
        SelectIndexes(plan); // Select best indexes
        DetermineExecutionStrategy(plan); // Determine execution strategy
        plan.EstimatedCost = CalculatePlanCost(plan); // Calculate estimated cost
        return plan;
    }

    void EstimateCardinalities(QueryPlan plan) {
        var query = plan.Query;
        var baseMetadata = _metadataProvider.GetObjectMetadata(query.FromObject);
        plan.BaseTableCardinality = _statisticsProvider.GetEstimatedRowCount(query.FromObject); // Base table cardinality
        plan.FilteredCardinality = query.WhereClause != null ? (int)(plan.BaseTableCardinality * EstimateSelectivity(query.WhereClause, baseMetadata)) : plan.BaseTableCardinality; // Apply WHERE clause selectivity
        // estimate join cardinalities
        if (query.Joins != null)
            foreach (var join in query.Joins) {
                //var targetMetadata = _metadataProvider.GetObjectMetadata(join.TargetObject);
                join.EstimatedRowCount = _statisticsProvider.GetEstimatedRowCount(join.TargetObject);
                // calculate join selectivity
                var fkField = baseMetadata.Fields.GetValueOrDefault(join.ForeignKey ?? "");
                if (fkField != null) join.Selectivity = fkField.Selectivity;
            }
    }

    static double EstimateSelectivity(SoqlCondition condition, ObjectMetadata metadata) {
        if (condition.IsCompound) {
            var leftSel = EstimateSelectivity(condition.Left!, metadata);
            var rightSel = EstimateSelectivity(condition.Right!, metadata);
            return condition.LogicalOp == LogicalOperator.AND
                ? leftSel * rightSel // AND reduces selectivity
                : leftSel + rightSel - (leftSel * rightSel); // OR increases selectivity
        }
        // get field metadata
        var field = metadata.Fields!.GetValueOrDefault(condition.Field);
        if (field == null) return 0.1; // Default selectivity for unknown fields
        return condition.Operator switch {
            SoqlOperator.Equals => field.Selectivity,
            SoqlOperator.NotEquals => 1.0 - field.Selectivity,
            SoqlOperator.LessThan => 0.33,
            SoqlOperator.LessThanOrEqual => 0.33,
            SoqlOperator.GreaterThan => 0.33,
            SoqlOperator.GreaterThanOrEqual => 0.33,
            SoqlOperator.Like => 0.1,
            SoqlOperator.Contains => 0.05,
            SoqlOperator.StartsWith => 0.1,
            SoqlOperator.EndsWith => 0.1,
            SoqlOperator.In => condition.Value is List<object> list ? Math.Min(0.5, list.Count * field.Selectivity) : 0.1,
            SoqlOperator.NotIn => condition.Value is List<object> list2 ? 1.0 - Math.Min(0.5, list2.Count * field.Selectivity) : 0.9,
            SoqlOperator.IsNull => field.IsNullable ? 0.1 : 0.0,
            SoqlOperator.IsNotNull => field.IsNullable ? 0.9 : 1.0,
            _ => 0.1
        };
    }

    void OptimizeJoinOrder(QueryPlan plan) {
        var joins = plan.Query.Joins!.ToList();
        if (joins.Count <= 1) { plan.OptimizedJoinOrder = joins; return; }
        plan.OptimizedJoinOrder = joins.Count <= 6
            ? FindOptimalJoinOrderDP(joins, plan.FilteredCardinality) // Use dynamic programming for optimal join order (for small number of joins)
            : FindOptimalJoinOrderGreedy(joins, plan.FilteredCardinality);// Use greedy algorithm for larger join sets
    }

    List<SoqlJoin> FindOptimalJoinOrderDP(List<SoqlJoin> joins, int baseCardinality) {
        // Dynamic programming approach to find optimal join order. This minimizes intermediate result sizes
        var n = joins.Count;
        var dp = new Dictionary<int, JoinOrderState>();
        // initialize with single joins
        for (var i = 0; i < n; i++) {
            var mask = 1 << i;
            var cost = CalculateJoinCost(baseCardinality, joins[i]);
            dp[mask] = new JoinOrderState { Cost = cost, Order = [joins[i]], Cardinality = (int)(baseCardinality * joins[i].Selectivity) };
        }
        // build up combinations
        for (var size = 2; size <= n; size++)
            foreach (var subset in GetSubsets(n, size)) {
                var minCost = double.MaxValue;
                List<SoqlJoin>? bestOrder = null;
                var bestCardinality = 0;
                // try adding each join in the subset
                for (var i = 0; i < n; i++) {
                    if ((subset & (1 << i)) == 0) continue;
                    var prevMask = subset & ~(1 << i);
                    if (!dp.ContainsKey(prevMask)) continue;
                    var prevState = dp[prevMask];
                    var cost = prevState.Cost + CalculateJoinCost(prevState.Cardinality, joins[i]);
                    if (cost < minCost) { minCost = cost; bestOrder = [.. prevState.Order, joins[i]]; bestCardinality = (int)(prevState.Cardinality * joins[i].Selectivity); }
                }
                if (bestOrder != null) dp[subset] = new JoinOrderState { Cost = minCost, Order = bestOrder, Cardinality = bestCardinality };
            }
        var fullMask = (1 << n) - 1;
        return dp.TryGetValue(fullMask, out JoinOrderState? value) ? value.Order : joins;
    }

    List<SoqlJoin> FindOptimalJoinOrderGreedy(List<SoqlJoin> joins, int baseCardinality) {
        // greedy algorithm: always pick the join that minimizes intermediate result size
        var remaining = new List<SoqlJoin>(joins);
        var res = new List<SoqlJoin>();
        var cardinality = baseCardinality;
        while (remaining.Count != 0) {
            SoqlJoin? bestJoin = null;
            var minCost = double.MaxValue;
            foreach (var join in remaining) { var cost = CalculateJoinCost(cardinality, join); if (cost < minCost) { minCost = cost; bestJoin = join; } }
            if (bestJoin != null) { res.Add(bestJoin); remaining.Remove(bestJoin); cardinality = (int)(cardinality * bestJoin.Selectivity); }
        }
        return res;
    }

    static double CalculateJoinCost(int leftCardinality, SoqlJoin join) {
        var joinCardinality = leftCardinality * join.EstimatedRowCount * join.Selectivity; // Cost model: combines intermediate result size with join complexity
        var nestedLoopCost = leftCardinality * join.EstimatedRowCount; // Nested loop join cost
        var hashJoinCost = leftCardinality + join.EstimatedRowCount; // Hash join cost (better for large tables)
        return Math.Min(nestedLoopCost, hashJoinCost) + joinCardinality; // Use minimum cost (optimizer will choose best strategy)
    }

    static IEnumerable<int> GetSubsets(int n, int size) => GenerateSubsets(n, size, 0, 0, size); // generate all subsets of given size
    static IEnumerable<int> GenerateSubsets(int n, int targetSize, int currentMask, int pos, int remaining) {
        if (remaining == 0) { yield return currentMask; yield break; }
        if (pos >= n) yield break;
        foreach (var subset in GenerateSubsets(n, targetSize, currentMask | (1 << pos), pos + 1, remaining - 1)) yield return subset; // Include current position
        foreach (var subset in GenerateSubsets(n, targetSize, currentMask, pos + 1, remaining)) yield return subset; // Exclude current position
    }

    void SelectIndexes(QueryPlan plan) {
        var metadata = _metadataProvider.GetObjectMetadata(plan.Query.FromObject);
        var candidateIndexes = new List<IndexCandidate>();
        if (plan.Query.WhereClause != null) CollectIndexCandidates(plan.Query.WhereClause, metadata, candidateIndexes);
        // score indexes based on selectivity and covering
        foreach (var candidate in candidateIndexes) { var field = metadata.Fields.GetValueOrDefault(candidate.Field); if (field?.IsIndexed == true) candidate.Score = 1.0 / (field.Selectivity + 0.01); } // Lower selectivity = higher score 
        plan.SelectedIndexes = [.. candidateIndexes.Where(c => c.Score > 0).OrderByDescending(c => c.Score).Take(3)]; // Limit to top 3 indexes
    }

    static void CollectIndexCandidates(SoqlCondition condition, ObjectMetadata metadata, List<IndexCandidate> candidates) {
        if (condition.IsCompound) { CollectIndexCandidates(condition.Left!, metadata, candidates); CollectIndexCandidates(condition.Right!, metadata, candidates); return; }
        if (!string.IsNullOrEmpty(condition.Field)) candidates.Add(new IndexCandidate { Field = condition.Field, Operator = condition.Operator });
    }

    static void DetermineExecutionStrategy(QueryPlan plan) {
        var query = plan.Query;
        if (query.Joins != null && query.Joins.Count >= 2 && plan.FilteredCardinality > 10000) { plan.UseParallelExecution = true; plan.ParallelDegree = Math.Min(4, Environment.ProcessorCount); }  // Parallel execution for large result sets with multiple joins
        if (query.GroupBy != null && query.GroupBy.Count != 0) plan.UseHashAggregation = true; // Use hash aggregation for GROUP BY
        if (plan.FilteredCardinality > 1000 && !query.IsAggregate) plan.UseStreaming = true; // Stream results for large datasets
    }

    static double CalculatePlanCost(QueryPlan plan) {
        var cardinality = plan.FilteredCardinality;
        var cost = plan.BaseTableCardinality * 0.1; // Base scan cost
        if (plan.Query.WhereClause != null) cost += plan.BaseTableCardinality * 0.05; // Add filter cost
        foreach (var join in plan.OptimizedJoinOrder) { cost += CalculateJoinCost(cardinality, join); cardinality = (int)(cardinality * join.Selectivity); } // Add join costs
        if (plan.Query.OrderBy != null && plan.Query.OrderBy.Count != 0) cost += cardinality * Math.Log(cardinality); // Add sort cost if needed
        if (plan.Query.IsAggregate) cost += cardinality * 0.1; // Add aggregation cost
        return cost;
    }

    // Generate a hash for caching query plans
    static string GeneratePlanHash(SoqlQuery query) => string.Join("|", new List<string> {
        query.FromObject,
        string.Join(",", query.SelectFields.Select(f => f.FieldName)),
        query.WhereClause?.ToString() ?? "",
        query.OrderBy != null ? string.Join(",", query.OrderBy.Select(o => $"{o.Field}:{o.Direction}")) : "",
        query.Limit?.ToString() ?? "",
        query.Offset?.ToString() ?? ""
    }).GetHashCode().ToString("X");
}

#endregion

#region Cache Plan

public class CachedPlan {
    public required QueryPlan Plan { get; set; }
    public required DateTime CreatedAt { get; set; }
    public required DateTime LastAccessedAt { get; set; }
    public long HitCount { get; set; }
}

public class CacheStatistics {
    public int TotalEntries { get; set; }
    public long TotalHits { get; set; }
    public double AverageHits { get; set; }
    public DateTime OldestEntry { get; set; }
    public DateTime MostRecentEntry { get; set; }
    public required List<PlanStatistic> TopPlans { get; set; }
}

public class PlanStatistic {
    public required string PlanId { get; set; }
    public long HitCount { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime LastAccessedAt { get; set; }
}

/// <summary>
/// Thread-safe cache for compiled query plans with LRU eviction
/// </summary>
public class QueryPlanCache {
    readonly ConcurrentDictionary<string, CachedPlan> _cache = new();
    readonly LinkedList<string> _lruList = new();
    readonly SemaphoreSlim _lock = new(1, 1);
    readonly int _maxSize;
    readonly TimeSpan _ttl;
    readonly Timer _cleanupTimer;

    public QueryPlanCache(int maxSize = 1000, TimeSpan? ttl = null) {
        _maxSize = maxSize;
        _ttl = ttl ?? TimeSpan.FromHours(1);
        _cleanupTimer = new Timer(_ => CleanupExpiredEntries(), null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5)); // Periodic cleanup of expired entries
    }

    public void Dispose() { _cleanupTimer?.Dispose(); _lock?.Dispose(); }

    /// <summary>
    /// Get a cached query plan or return null if not found
    /// </summary>
    public QueryPlan? Get(string queryHash) {
        if (_cache.TryGetValue(queryHash, out var cached)) {
            if (DateTime.UtcNow - cached.CreatedAt < _ttl) { cached.LastAccessedAt = DateTime.UtcNow; cached.HitCount++; UpdateLru(queryHash); return cached.Plan; }
            else _cache.TryRemove(queryHash, out _); // Expired
        }
        return null;
    }

    /// <summary>
    /// Add or update a query plan in the cache
    /// </summary>
    public async Task<bool> SetAsync(string queryHash, QueryPlan plan) {
        await _lock.WaitAsync();
        try {
            // Check if we need to evict
            if (_cache.Count >= _maxSize) EvictLru();
            _cache[queryHash] = new CachedPlan { Plan = plan, CreatedAt = DateTime.UtcNow, LastAccessedAt = DateTime.UtcNow, HitCount = 0 };
            _lruList.AddFirst(queryHash);
            return true;
        }
        finally { _lock.Release(); }
    }

    /// <summary>
    /// Remove a specific query plan from cache
    /// </summary>
    public bool Remove(string queryHash) {
        if (_cache.TryRemove(queryHash, out _)) { RemoveFromLru(queryHash); return true; }
        return false;
    }

    /// <summary>
    /// Clear all cached plans
    /// </summary>
    public async Task ClearAsync() {
        await _lock.WaitAsync();
        try { _cache.Clear(); _lruList.Clear(); }
        finally { _lock.Release(); }
    }

    /// <summary>
    /// Get cache statistics
    /// </summary>
    public CacheStatistics GetStatistics() {
        var plans = _cache.Values.ToList();
        return new CacheStatistics {
            TotalEntries = _cache.Count,
            TotalHits = plans.Sum(p => p.HitCount),
            AverageHits = plans.Count != 0 ? plans.Average(p => p.HitCount) : 0,
            OldestEntry = plans.Count != 0 ? plans.Min(p => p.CreatedAt) : DateTime.UtcNow,
            MostRecentEntry = plans.Count != 0 ? plans.Max(p => p.CreatedAt) : DateTime.UtcNow,
            TopPlans = [.. plans.OrderByDescending(p => p.HitCount).Take(10).Select(p => new PlanStatistic { PlanId = p.Plan.PlanId, HitCount = p.HitCount, CreatedAt = p.CreatedAt, LastAccessedAt = p.LastAccessedAt })]
        };
    }

    void UpdateLru(string queryHash) {
        _lock.Wait();
        try {
            var node = _lruList.Find(queryHash);
            if (node != null) { _lruList.Remove(node); _lruList.AddFirst(node); }
        }
        finally { _lock.Release(); }
    }

    void RemoveFromLru(string queryHash) {
        _lock.Wait();
        try { _lruList.Remove(queryHash); }
        finally { _lock.Release(); }
    }

    void EvictLru() {
        if (_lruList.Last != null) { var toEvict = _lruList.Last.Value; _cache.TryRemove(toEvict, out _); _lruList.RemoveLast(); }
    }

    void CleanupExpiredEntries() {
        _lock.Wait();
        var now = DateTime.UtcNow;
        try { foreach (var key in _cache.Where(kvp => now - kvp.Value.CreatedAt >= _ttl).Select(kvp => kvp.Key)) { _cache.TryRemove(key, out _); _lruList.Remove(key); } }
        finally { _lock.Release(); }
    }
}

#endregion

#region Cache Results

public class CachedResultSet {
    public required List<object> Results { get; set; }
    public DateTime CreatedAt { get; set; }
    public long HitCount { get; set; }
}

/// <summary>
/// Result set cache for frequently accessed queries
/// </summary>
public class ResultSetCache(int maxSize = 100, TimeSpan? ttl = null) {
    readonly ConcurrentDictionary<string, CachedResultSet> _cache = new();
    readonly int _maxSize = maxSize;
    readonly TimeSpan _ttl = ttl ?? TimeSpan.FromMinutes(5);

    /// <summary>
    /// Get cached results or return null
    /// </summary>
    public List<object>? Get(string queryHash) {
        if (_cache.TryGetValue(queryHash, out var cached))
            if (DateTime.UtcNow - cached.CreatedAt < _ttl) { cached.HitCount++; return cached.Results; }
            else _cache.TryRemove(queryHash, out _);
        return null;
    }

    /// <summary>
    /// Cache query results
    /// </summary>
    public bool Set(string queryHash, List<object> results) {
        if (results.Count > 1000) return false; // Don't cache very large result sets
        if (_cache.Count >= _maxSize) {
            // Remove oldest entry
            var oldest = _cache.OrderBy(kvp => kvp.Value.CreatedAt).FirstOrDefault();
            if (oldest.Key != null) _cache.TryRemove(oldest.Key, out _);
        }
        _cache[queryHash] = new CachedResultSet { Results = results, CreatedAt = DateTime.UtcNow, HitCount = 0 };
        return true;
    }

    /// <summary>
    /// Invalidate cached results based on object name
    /// </summary>
    public void InvalidateByObject(string objectName) {
        foreach (var key in _cache.Where(kvp => kvp.Key.Contains(objectName)).Select(kvp => kvp.Key)) _cache.TryRemove(key, out _);
    }

    public void Clear() => _cache.Clear();
}

#endregion