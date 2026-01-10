using Dapper;
using Open.Soql.Query;
using System.Collections.Concurrent;
using System.Data;

namespace Open.Soql.Metadata;

#region Model

public enum DatabaseProvider { Postgres, SqlServer, Mock }

/// <summary>
/// Represents metadata about a CRM object (table)
/// </summary>
public class ObjectMetadata {
    public required string ObjectName { get; set; }
    public required string TableName { get; set; }
    public Dictionary<string, FieldMetadata> Fields { get; set; } = [];
    public List<RelationshipMetadata> Relationships { get; set; } = [];
    public bool HasRls { get; set; }
    public string? RlsPolicy { get; set; }
    public int EstimatedRowCount { get; set; }
}

public class FieldMetadata {
    public string? FieldName { get; set; }
    public string? ColumnName { get; set; }
    public Type? DataType { get; set; }
    public bool IsNullable { get; set; }
    public bool IsIndexed { get; set; }
    public double Selectivity { get; set; } = 1.0;
}

public class RelationshipMetadata {
    public required string RelationshipName { get; set; }
    public required string TargetObject { get; set; }
    public required string ForeignKeyField { get; set; }
    public required string ReferencedKeyField { get; set; }
    public RelationshipType Type { get; set; }
}

public enum RelationshipType { Lookup, MasterDetail, ManyToMany }

public class SecurityContext {
    public required string UserId { get; set; }
    public string? Username { get; set; }
    public List<string> Roles { get; set; } = [];
    public List<string> Permissions { get; set; } = [];
    public List<string> TerritoryIds { get; set; } = [];
    public string? ProfileId { get; set; }
    public Dictionary<string, object> CustomAttributes { get; set; } = [];
}

#endregion

#region Providers

public interface IMetadataProvider {
    ObjectMetadata GetObjectMetadata(string objectName);
}

public interface IStatisticsProvider {
    int GetEstimatedRowCount(string objectName);
    double GetFieldSelectivity(string objectName, string fieldName);
}

public interface ISecurityContextProvider {
    SecurityContext GetCurrentContext();
}

/// <summary>
/// Database-backed metadata provider
/// </summary>
public class DatabaseMetadataProvider(IDbConnection? connection, DatabaseProvider provider) : IMetadataProvider {
    readonly IDbConnection? _connection = connection;
    readonly DatabaseProvider _provider = provider;
    readonly Dictionary<string, ObjectMetadata> _cache = [];

    public ObjectMetadata GetObjectMetadata(string objectName) {
        if (_cache.TryGetValue(objectName, out var cached)) return cached;
        var metadata = LoadMetadata(objectName);
        _cache[objectName] = metadata;
        return metadata;
    }

    ObjectMetadata LoadMetadata(string objectName) {
        var tableName = ConvertToTableName(objectName);
        return new ObjectMetadata {
            ObjectName = objectName,
            TableName = tableName,
            Fields = LoadFields(tableName),                 // Load fields
            Relationships = LoadRelationships(objectName),  // Load relationships
            HasRls = CheckRlsEnabled(tableName),            // Check for RLS
            EstimatedRowCount = EstimateRowCount(tableName), // Estimate row count
        };
    }

    Dictionary<string, FieldMetadata> LoadFields(string tableName) {
        var sql = _provider switch {
            DatabaseProvider.Postgres => $@"SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = @tableName ORDER BY ordinal_position",
            DatabaseProvider.SqlServer => $@"SELECT c.name as column_name, t.name as data_type, c.is_nullable, object_definition(c.default_object_id) as column_default FROM sys.columns c INNER JOIN sys.types t ON c.user_type_id = t.user_type_id INNER JOIN sys.tables tb ON c.object_id = tb.object_id WHERE tb.name = @tableName ORDER BY c.column_id",
            _ => throw new NotSupportedException($"Provider {_provider} not supported")
        };
        var results = _connection.Query<dynamic>(sql, new { tableName });
        var fields = new Dictionary<string, FieldMetadata>();
        foreach (var row in results) {
            var columnName = (string)row.column_name;
            var fieldName = ConvertToFieldName(columnName);
            fields[fieldName] = new FieldMetadata {
                FieldName = fieldName,
                ColumnName = columnName,
                DataType = MapDataType((string)row.data_type),
                IsNullable = MapNullable(row.is_nullable),
                IsIndexed = CheckIndexExists(tableName, columnName),
                Selectivity = EstimateSelectivity(tableName, columnName)
            };
        }
        return fields;
    }

    List<RelationshipMetadata> LoadRelationships(string objectName) {
        var relationships = new List<RelationshipMetadata>();
        var sql = _provider switch {
            DatabaseProvider.Postgres => $@"SELECT kcu.column_name as fk_column, ccu.table_name as referenced_table, ccu.column_name as referenced_column FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = @tableName",
            DatabaseProvider.SqlServer => $@"SELECT fk.name as constraint_name, c1.name as fk_column, t2.name as referenced_table, c2.name as referenced_column FROM sys.foreign_keys fk INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id INNER JOIN sys.columns c1 ON fkc.parent_object_id = c1.object_id AND fkc.parent_column_id = c1.column_id INNER JOIN sys.columns c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id INNER JOIN sys.tables t1 ON fk.parent_object_id = t1.object_id INNER JOIN sys.tables t2 ON fk.referenced_object_id = t2.object_id WHERE t1.name = @tableName",
            _ => throw new NotSupportedException()
        };
        var results = _connection.Query<dynamic>(sql, new { tableName = ConvertToTableName(objectName) });
        foreach (var row in results) {
            var fkColumn = ConvertToFieldName((string)row.fk_column);
            var refTable = (string)row.referenced_table;
            var refColumn = ConvertToFieldName((string)row.referenced_column);
            var relationshipName = fkColumn.EndsWith("Id") ? fkColumn[..^2] : fkColumn; // Generate relationship name (e.g., Account for AccountId field)
            relationships.Add(new RelationshipMetadata {
                RelationshipName = relationshipName,
                TargetObject = ConvertToObjectName(refTable),
                ForeignKeyField = fkColumn,
                ReferencedKeyField = refColumn,
                Type = RelationshipType.Lookup
            });
        }
        return relationships;
    }

    bool CheckRlsEnabled(string tableName) {
        if (_provider == DatabaseProvider.Postgres) {
            var sql = @"SELECT relrowsecurity FROM pg_class WHERE relname = @tableName";
            var result = _connection.QueryFirstOrDefault<bool?>(sql, new { tableName });
            return result ?? false;
        }
        return false;
    }

    bool CheckIndexExists(string tableName, string columnName) {
        var sql = _provider switch {
            DatabaseProvider.Postgres => $@"SELECT COUNT(*) > 0 FROM pg_indexes WHERE tablename = @tableName AND indexdef LIKE '%' || @name || '%'",
            DatabaseProvider.SqlServer => $@"SELECT COUNT(*) FROM sys.indexes i INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id INNER JOIN sys.tables t ON i.object_id = t.object_id WHERE t.name = @tableName AND c.name = @name",
            _ => throw new NotSupportedException()
        };
        return _connection.QueryFirstOrDefault<int>(sql, new { tableName, columnName }) > 0;
    }

    int EstimateRowCount(string tableName) {
        var sql = _provider switch {
            DatabaseProvider.Postgres => $@"SELECT reltuples::bigint FROM pg_class WHERE relname = @tableName",
            DatabaseProvider.SqlServer => $@"SELECT SUM(row_count) FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID(@tableName) AND index_id IN (0, 1)",
            _ => $"SELECT COUNT(*) FROM {tableName}"
        };
        return _connection.QueryFirstOrDefault<int?>(sql, new { tableName }) ?? 1000;
    }

    double EstimateSelectivity(string tableName, string columnName) {
        try {
            var sql = _provider switch {
                DatabaseProvider.Postgres => $@"SELECT 1.0 / n_distinct FROM pg_stats WHERE tablename = @tableName AND attname = @name",
                _ => null
            };
            if (sql != null) {
                var result = _connection.QueryFirstOrDefault<double?>(sql, new { tableName, columnName });
                return result ?? 0.1;
            }
        }
        catch { }
        return 0.1; // Default selectivity
    }

    static Type MapDataType(string dbType) => dbType.ToLower() switch {
        "integer" or "int" or "bigint" => typeof(long),
        "smallint" => typeof(short),
        "decimal" or "numeric" or "money" => typeof(decimal),
        "real" or "float" or "double precision" => typeof(double),
        "boolean" or "bit" => typeof(bool),
        "date" or "timestamp" or "datetime" or "datetime2" => typeof(DateTime),
        "uuid" or "uniqueidentifier" => typeof(Guid),
        _ => typeof(string)
    };

    static bool MapNullable(object value) => value switch {
        bool b => b,
        string s => s.Equals("YES", StringComparison.OrdinalIgnoreCase),
        _ => false
    };
    static string ConvertToTableName(string objectName) => Globalx.PascalCaseToSnakeCase(objectName);
    static string ConvertToFieldName(string columnName) => Globalx.SnakeCaseToPascalCase(columnName);
    static string ConvertToObjectName(string tableName) => Globalx.SnakeCaseToPascalCase(tableName);
}

/// <summary>
/// Mock metadata provider for testing
/// </summary>
public class MockMetadataProvider : IMetadataProvider {
    readonly Dictionary<string, ObjectMetadata> _metadata = [];

    public MockMetadataProvider() => InitializeMockData();
    public ObjectMetadata GetObjectMetadata(string objectName) => _metadata.GetValueOrDefault(objectName) ?? CreateDefaultMetadata(objectName);
    public void AddMetadata(ObjectMetadata metadata) => _metadata[metadata.ObjectName] = metadata;
    void InitializeMockData() {
        // Account object
        var account = new ObjectMetadata {
            ObjectName = "Account",
            TableName = "accounts",
            HasRls = true,
            EstimatedRowCount = 10000,
            Fields = new Dictionary<string, FieldMetadata> {
                ["Id"] = new FieldMetadata { FieldName = "Id", ColumnName = "id", DataType = typeof(Guid), IsIndexed = true, Selectivity = 1.0 / 10000 },
                ["Name"] = new FieldMetadata { FieldName = "Name", ColumnName = "name", DataType = typeof(string), IsIndexed = true, Selectivity = 0.1 },
                ["OwnerId"] = new FieldMetadata { FieldName = "OwnerId", ColumnName = "owner_id", DataType = typeof(Guid), IsIndexed = true, Selectivity = 0.01 },
                ["CreatedDate"] = new FieldMetadata { FieldName = "CreatedDate", ColumnName = "created_date", DataType = typeof(DateTime), Selectivity = 0.001 }
            },
            Relationships = []
        };
        _metadata["Account"] = account;

        // Contact object
        var contact = new ObjectMetadata {
            ObjectName = "Contact",
            TableName = "contacts",
            HasRls = true,
            EstimatedRowCount = 50000,
            Fields = new Dictionary<string, FieldMetadata> {
                ["Id"] = new FieldMetadata { FieldName = "Id", ColumnName = "id", DataType = typeof(Guid), IsIndexed = true, Selectivity = 1.0 / 50000 },
                ["FirstName"] = new FieldMetadata { FieldName = "FirstName", ColumnName = "first_name", DataType = typeof(string), Selectivity = 0.05 },
                ["LastName"] = new FieldMetadata { FieldName = "LastName", ColumnName = "last_name", DataType = typeof(string), IsIndexed = true, Selectivity = 0.02 },
                ["AccountId"] = new FieldMetadata { FieldName = "AccountId", ColumnName = "account_id", DataType = typeof(Guid), IsIndexed = true, Selectivity = 0.002 },
                ["OwnerId"] = new FieldMetadata { FieldName = "OwnerId", ColumnName = "owner_id", DataType = typeof(Guid), IsIndexed = true, Selectivity = 0.01 }
            },
            Relationships = [
                new RelationshipMetadata {
                    RelationshipName = "Account",
                    TargetObject = "Account",
                    ForeignKeyField = "AccountId",
                    ReferencedKeyField = "Id",
                    Type = RelationshipType.Lookup
                }
            ]
        };
        _metadata["Contact"] = contact;
    }

    static ObjectMetadata CreateDefaultMetadata(string objectName) => new() {
        ObjectName = objectName,
        TableName = objectName.ToLower(),
        EstimatedRowCount = 1000,
        Fields = new Dictionary<string, FieldMetadata> {
            ["Id"] = new FieldMetadata { FieldName = "Id", ColumnName = "id", DataType = typeof(Guid), IsIndexed = true, Selectivity = 0.001 }
        }
    };
}

/// <summary>
/// Statistics provider for query optimization
/// </summary>
public class DatabaseStatisticsProvider(IDbConnection connection, DatabaseProvider provider) : IStatisticsProvider {
    readonly IDbConnection _connection = connection;
    readonly DatabaseProvider _provider = provider;

    public int GetEstimatedRowCount(string objectName) {
        var tableName = ConvertToTableName(objectName);
        var sql = _provider switch {
            DatabaseProvider.Postgres => $@"SELECT reltuples::bigint FROM pg_class WHERE relname = @tableName",
            DatabaseProvider.SqlServer => $@"SELECT SUM(row_count) FROM sys.dm_db_partition_stats ps INNER JOIN sys.tables t ON ps.object_id = t.object_id WHERE t.name = @tableName AND ps.index_id IN (0, 1)",
            _ => $"SELECT COUNT(*) FROM {tableName}"
        };
        return _connection.QueryFirstOrDefault<int?>(sql, new { tableName }) ?? 1000;
    }

    public double GetFieldSelectivity(string objectName, string fieldName) {
        var tableName = ConvertToTableName(objectName);
        var columnName = ConvertToColumnName(fieldName);
        if (_provider == DatabaseProvider.Postgres) {
            var sql = @"SELECT CASE WHEN n_distinct < 0 THEN ABS(n_distinct) ELSE 1.0 / NULLIF(n_distinct, 0) END as selectivity FROM pg_stats WHERE tablename = @tableName AND attname = @name";
            return _connection.QueryFirstOrDefault<double?>(sql, new { tableName, columnName }) ?? 0.1;
        }
        return 0.1; // Default selectivity
    }

    static string ConvertToTableName(string objectName) => Globalx.PascalCaseToSnakeCase(objectName);
    static string ConvertToColumnName(string fieldName) => Globalx.PascalCaseToSnakeCase(fieldName);
}

/// <summary>
/// Mock statistics provider for testing
/// </summary>
public class MockStatisticsProvider : IStatisticsProvider {
    readonly Dictionary<string, int> _rowCounts = [];
    public int GetEstimatedRowCount(string objectName) => _rowCounts.GetValueOrDefault(objectName, 1000);
    public double GetFieldSelectivity(string objectName, string fieldName) => 0.1; // Default 10% selectivity
    public MockStatisticsProvider SetRowCount(string objectName, int count) { _rowCounts[objectName] = count; return this; }
}

/// <summary>
/// Mock security context provider for testing
/// </summary>
public class MockSecurityContextProvider : ISecurityContextProvider {
    SecurityContext _context;
    public MockSecurityContextProvider(SecurityContext? context = null) {
        _context = context ?? new SecurityContext {
            UserId = "test-user-id",
            Username = "testuser",
            Roles = ["StandardUser"],
            Permissions = ["Read", "Create", "Edit"]
        };
    }
    public SecurityContext GetCurrentContext() => _context;
    public void SetContext(SecurityContext context) => _context = context;
}

#endregion

#region Rls Provider

/// <summary>
/// Postgres-specific RLS implementation using native database policies
/// </summary>
public class PostgresRlsProvider {
    /// <summary>
    /// Generate Postgres RLS policy SQL
    /// </summary>
    public string GeneratePolicy(string objectName, RlsPolicy policy, string tableName) {
        var policyName = $"rls_{objectName}_{policy.Name}".ToLower();
        var sql = $@"-- Enable RLS on table
ALTER TABLE {tableName} ENABLE ROW LEVEL SECURITY;
-- Drop existing policy if exists
DROP POLICY IF EXISTS {policyName} ON {tableName};
-- Create RLS policy
CREATE POLICY {policyName} ON {tableName} FOR ALL TO PUBLIC USING ({GeneratePolicyCondition(policy, tableName)});
-- Grant SELECT to application role
GRANT SELECT ON {tableName} TO crm_app_role;
";
        return sql;
    }

    static string GeneratePolicyCondition(RlsPolicy policy, string tableName) => policy.PolicyType switch {
        RlsPolicyType.OwnerBased => $"{tableName}.owner_id = current_setting('app.current_user_id')::uuid",
        RlsPolicyType.SharingBased => $@"({tableName}.owner_id = current_setting('app.current_user_id')::uuid OR EXISTS (SELECT 1 FROM sharing_rules sr WHERE sr.record_id = {tableName}.id AND sr.user_id = current_setting('app.current_user_id')::uuid))",
        RlsPolicyType.HierarchyBased => $@"EXISTS (SELECT 1 FROM user_role_hierarchy urh WHERE urh.subordinate_user_id = {tableName}.owner_id AND urh.supervisor_user_id = current_setting('app.current_user_id')::uuid)",
        RlsPolicyType.TerritoryBased => $@"EXISTS (SELECT 1 FROM user_territories ut WHERE ut.territory_id = {tableName}.territory_id AND ut.user_id = current_setting('app.current_user_id')::uuid)",
        _ => "true"
    };

    /// <summary>
    /// Set session variables for Postgres RLS
    /// </summary>
    public string GetSessionSql(SecurityContext context) => $@"SET LOCAL app.current_user_id = '{context.UserId}'; SET LOCAL app.current_role = '{string.Join(",", context.Roles)}';";
}

#endregion

#region Cache

/// <summary>
/// Metadata cache for object and field information
/// </summary>
public class MetadataCache {
    readonly ConcurrentDictionary<string, ObjectMetadata> _objectCache = new();
    readonly Func<string, Task<ObjectMetadata>> _loader;
    readonly TimeSpan _ttl;
    readonly Timer _refreshTimer;

    public MetadataCache(Func<string, Task<ObjectMetadata>> loader, TimeSpan? ttl = null) {
        _loader = loader;
        _ttl = ttl ?? TimeSpan.FromMinutes(30);
        _refreshTimer = new Timer(async _ => await RefreshAllAsync(), null, _ttl, _ttl); // Periodic refresh
    }

    public void Dispose() => _refreshTimer?.Dispose();

    /// <summary>
    /// Get object metadata with caching
    /// </summary>
    public async Task<ObjectMetadata> GetAsync(string objectName) {
        if (_objectCache.TryGetValue(objectName, out var metadata)) return metadata;
        metadata = await _loader(objectName);
        _objectCache[objectName] = metadata;
        return metadata;
    }

    /// <summary>
    /// Invalidate cache for a specific object
    /// </summary>
    public void Invalidate(string objectName) => _objectCache.TryRemove(objectName, out _);

    /// <summary>
    /// Refresh all cached metadata
    /// </summary>
    public async Task RefreshAllAsync() {
        var objectNames = _objectCache.Keys.ToList();
        var refreshTasks = objectNames.Select(async name => {
            try { _objectCache[name] = await _loader(name); }
            catch { } // Keep old metadata on refresh failure
        });
        await Task.WhenAll(refreshTasks);
    }
}

#endregion