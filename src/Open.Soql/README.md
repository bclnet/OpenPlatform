# CRM SOQL Execution Engine

A production-ready Salesforce-like Object Query Language (SOQL) execution engine for C# with Dapper, supporting Postgres, SQL Server, and testing mocks.

## Features

### Core Functionality
- ✅ **SOQL Parser** - Full SOQL syntax support with relationship traversal, aggregates, and subqueries
- ✅ **Query Optimizer** - Cost-based join reordering and execution strategy selection
- ✅ **SQL Compilation** - Native SQL generation for Postgres and SQL Server
- ✅ **Expression Trees** - Compiled expression trees for high-performance result mapping
- ✅ **Query Plan Caching** - LRU cache with TTL for optimized query plans
- ✅ **Result Set Caching** - Optional caching for frequently accessed data

### Advanced Features
- ✅ **Row-Level Security** - User, role, hierarchy, and territory-based access control
- ✅ **Native Postgres RLS** - Database-level policy enforcement
- ✅ **Parallel Relationship Loading** - Async loading of related records
- ✅ **Streaming Execution** - Memory-efficient processing of large result sets
- ✅ **Batch Query Execution** - Multiple queries in single database round-trip
- ✅ **Aggregate Functions** - COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT
- ✅ **Subquery Support** - IN, NOT IN, and correlated subqueries

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      SOQL Engine                             │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────┐ │
│  │  Parser  │──>│Optimizer │──>│Generator │──>│Executor │ │
│  └──────────┘   └──────────┘   └──────────┘   └─────────┘ │
│       │              │               │              │       │
│       v              v               v              v       │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────┐ │
│  │   RLS    │   │Plan Cache│   │ Metadata │   │ Dapper  │ │
│  │ Enforcer │   └──────────┘   │ Provider │   │         │ │
│  └──────────┘                  └──────────┘   └─────────┘ │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                           │
                           v
        ┌─────────────────────────────────────┐
        │  Database (Postgres / SQL Server)   │
        └─────────────────────────────────────┘
```

## Installation

```bash
dotnet add package Dapper
dotnet add package Npgsql  # For Postgres
dotnet add package System.Data.SqlClient  # For SQL Server
```

## Quick Start

### Basic Setup

```csharp
using CrmSoql.Engine;
using CrmSoql.Providers;
using CrmSoql.Compilation;
using Npgsql;

// Create database connection
var connection = new NpgsqlConnection(connectionString);

// Setup providers
var metadataProvider = new DatabaseMetadataProvider(connection, DatabaseProvider.Postgres);
var statsProvider = new DatabaseStatisticsProvider(connection, DatabaseProvider.Postgres);
var securityProvider = new MockSecurityContextProvider(new SecurityContext 
{ 
    UserId = "current-user-id" 
});

// Create SOQL engine
var engine = new SoqlEngine(
    DatabaseProvider.Postgres,
    connection,
    metadataProvider,
    statsProvider,
    securityProvider,
    new SoqlEngineOptions
    {
        EnableRowLevelSecurity = true,
        EnablePlanCaching = true,
        EnableParallelExecution = true
    }
);

// Execute queries
var accounts = await engine.QueryAsync<Account>(
    "SELECT Id, Name, AnnualRevenue FROM Account WHERE Industry = 'Technology' ORDER BY Name LIMIT 10"
);
```

### Simple Query

```csharp
var contacts = await engine.QueryAsync<Contact>(
    "SELECT Id, FirstName, LastName FROM Contact WHERE CreatedDate > 2024-01-01"
);
```

### Relationship Traversal

```csharp
var contacts = await engine.QueryAsync<Contact>(@"
    SELECT 
        Id, 
        FirstName, 
        LastName, 
        Account.Name,
        Account.Industry
    FROM Contact 
    WHERE Account.AnnualRevenue > 1000000
");
```

### Aggregate Queries

```csharp
var results = await engine.QueryAsync(@"
    SELECT 
        StageName,
        COUNT(Id) as Count,
        SUM(Amount) as TotalAmount,
        AVG(Amount) as AvgAmount
    FROM Opportunity
    WHERE CloseDate >= 2024-01-01
    GROUP BY StageName
    HAVING COUNT(Id) > 5
    ORDER BY TotalAmount DESC
");
```

### Subqueries

```csharp
var accounts = await engine.QueryAsync(@"
    SELECT Id, Name
    FROM Account
    WHERE Id IN (
        SELECT AccountId 
        FROM Opportunity 
        WHERE StageName = 'Closed Won'
        AND CloseDate >= 2024-01-01
    )
");
```

## SOQL Syntax Support

### SELECT Clause
```sql
SELECT Id, Name, CustomField__c
SELECT COUNT(Id), SUM(Amount)
SELECT Account.Name, Account.Owner.Name
SELECT (SELECT Name FROM Contacts) FROM Account
```

### FROM Clause
```sql
FROM Account
FROM CustomObject__c
```

### WHERE Clause
```sql
WHERE Name = 'Acme'
WHERE Amount > 1000
WHERE CreatedDate >= 2024-01-01
WHERE Name LIKE '%Corp%'
WHERE Id IN ('id1', 'id2')
WHERE AccountId IN (SELECT Id FROM Account WHERE Industry = 'Tech')
WHERE IsActive = true AND (Priority = 'High' OR Priority = 'Critical')
```

### Operators
- Comparison: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- Pattern: `LIKE`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`
- List: `IN`, `NOT IN`
- Null: `IS NULL`, `IS NOT NULL`
- Logical: `AND`, `OR`

### GROUP BY & HAVING
```sql
GROUP BY Industry
GROUP BY Industry, Type
HAVING COUNT(Id) > 10
HAVING SUM(Amount) > 100000
```

### ORDER BY
```sql
ORDER BY Name
ORDER BY CreatedDate DESC
ORDER BY Name ASC NULLS FIRST
ORDER BY Amount DESC, Name ASC
```

### LIMIT & OFFSET
```sql
LIMIT 100
LIMIT 50 OFFSET 100
```

### Aggregate Functions
- `COUNT(field)`
- `COUNT(DISTINCT field)`
- `SUM(field)`
- `AVG(field)`
- `MIN(field)`
- `MAX(field)`

## Row-Level Security

### Default Policies

The engine includes several built-in RLS policies:

```csharp
// Owner-based: Users can only see records they own
"WHERE OwnerId = @currentUserId"

// Sharing-based: Users can see records shared with them
"WHERE OwnerId = @currentUserId OR Id IN (SELECT RecordId FROM Share WHERE UserId = @currentUserId)"

// Hierarchy-based: Users can see subordinate records
"WHERE OwnerId IN (SELECT SubordinateUserId FROM Hierarchy WHERE SupervisorUserId = @currentUserId)"

// Territory-based: Users can see records in their territories
"WHERE TerritoryId IN (@userTerritories)"
```

### Custom RLS Policies

```csharp
var rls = new RowLevelSecurityEnforcer(securityProvider, metadataProvider);

rls.RegisterPolicy("Account", new RlsPolicy
{
    Name = "CustomPolicy",
    PolicyType = RlsPolicyType.Custom,
    Condition = ctx => new SoqlCondition
    {
        Field = "Region",
        Operator = SoqlOperator.In,
        Value = ctx.CustomAttributes["AllowedRegions"]
    }
});
```

### Postgres Native RLS

```csharp
var postgresRls = new PostgresRlsProvider();

// Generate RLS policy SQL
var policySql = postgresRls.GeneratePostgresRlsPolicy(
    "Account", 
    policy, 
    "accounts"
);

await connection.ExecuteAsync(policySql);

// Set session variables
var sessionSql = postgresRls.GetSessionConfigurationSql(securityContext);
await connection.ExecuteAsync(sessionSql);
```

## Query Optimization

### Cost-Based Join Reordering

The optimizer uses dynamic programming (for ≤6 joins) or greedy algorithms (for >6 joins) to find optimal join orders:

```csharp
// Original query
SELECT c.Id, c.Name, a.Name, o.Amount
FROM Contact c
JOIN Account a ON c.AccountId = a.Id
JOIN Opportunity o ON c.Id = o.ContactId

// Optimizer may reorder to:
// 1. Start with smallest table
// 2. Apply most selective joins first
// 3. Minimize intermediate result sizes
```

### Index Selection

The optimizer analyzes available indexes and selects the most beneficial ones based on:
- Field selectivity
- Operator type
- Covering indexes

### Execution Strategies

```csharp
// Parallel execution for large datasets
if (estimatedRows > 10000 && joinCount >= 2)
    plan.UseParallelExecution = true;

// Hash aggregation for GROUP BY
if (hasGroupBy)
    plan.UseHashAggregation = true;

// Streaming for memory efficiency
if (estimatedRows > 1000)
    plan.UseStreaming = true;
```

## Caching

### Query Plan Cache

```csharp
// Plans are cached based on query structure
var plan = await planCache.SetAsync(queryHash, optimizedPlan);

// Automatic LRU eviction
var options = new SoqlEngineOptions
{
    PlanCacheSize = 1000,
    PlanCacheTtl = TimeSpan.FromHours(1)
};
```

### Result Set Cache

```csharp
// Optional caching for frequently accessed queries
var options = new SoqlEngineOptions
{
    EnableResultCaching = true,
    ResultCacheSize = 100,
    ResultCacheTtl = TimeSpan.FromMinutes(5),
    MaxResultCacheSize = 1000  // Don't cache very large results
};

// Invalidate on data changes
engine.InvalidateCache("Account");
```

### Cache Statistics

```csharp
var stats = engine.GetCacheStatistics();
Console.WriteLine($"Total Hits: {stats.TotalHits}");
Console.WriteLine($"Average Hits: {stats.AverageHits}");
Console.WriteLine($"Cache Size: {stats.TotalEntries}");
```

## Parallel Relationship Loading

```csharp
// Relationships are loaded in parallel when enabled
var contacts = await engine.QueryAsync<Contact>(@"
    SELECT 
        Id, 
        Name, 
        Account.Name,
        Account.Owner.Name,
        CreatedBy.Name
    FROM Contact
    LIMIT 1000
");

// Three relationships loaded in parallel:
// 1. Account
// 2. Account.Owner
// 3. CreatedBy
```

## Query Explanation

```csharp
var plan = await engine.ExplainAsync(@"
    SELECT Id, Account.Name
    FROM Contact
    WHERE CreatedDate > 2024-01-01
    ORDER BY CreatedDate DESC
");

Console.WriteLine($"Estimated Cost: {plan.EstimatedCost}");
Console.WriteLine($"Base Rows: {plan.BaseTableCardinality}");
Console.WriteLine($"Filtered Rows: {plan.FilteredCardinality}");
Console.WriteLine($"Parallel: {plan.UseParallelExecution}");

foreach (var join in plan.OptimizedJoinOrder)
{
    Console.WriteLine($"Join: {join.RelationshipName} -> {join.TargetObject}");
}
```

## Performance Tuning

### 1. Enable Appropriate Caching

```csharp
var options = new SoqlEngineOptions
{
    EnablePlanCaching = true,      // Always recommended
    EnableResultCaching = false,   // Only for read-heavy workloads
    EnableParallelExecution = true // For queries with multiple joins
};
```

### 2. Use Streaming for Large Results

```csharp
var executor = new StreamingQueryExecutor(connection, provider);
await foreach (var record in executor.ExecuteStreamingAsync<Account>(compiledQuery))
{
    // Process one record at a time
}
```

### 3. Batch Multiple Queries

```csharp
var batchExecutor = new BatchQueryExecutor(connection, executor);
var queries = new Dictionary<string, CompiledQuery>
{
    ["accounts"] = compiledAccountQuery,
    ["contacts"] = compiledContactQuery,
    ["opportunities"] = compiledOpportunityQuery
};

var results = await batchExecutor.ExecuteBatchAsync(queries);
```

### 4. Database Indexes

Ensure indexes exist on:
- Primary keys
- Foreign keys
- Frequently filtered fields
- ORDER BY fields

### 5. Statistics Updates

Keep database statistics up-to-date for accurate cardinality estimation:

```sql
-- Postgres
ANALYZE accounts;

-- SQL Server
UPDATE STATISTICS accounts;
```

## Testing

### Mock Providers

```csharp
var metadataProvider = new MockMetadataProvider();
var statsProvider = new MockStatisticsProvider();
var securityProvider = new MockSecurityContextProvider();

// Add custom metadata
metadataProvider.AddMetadata(new ObjectMetadata
{
    ObjectName = "CustomObject",
    TableName = "custom_objects",
    Fields = new Dictionary<string, FieldMetadata>
    {
        ["Id"] = new FieldMetadata { FieldName = "Id", DataType = typeof(Guid) },
        ["Name"] = new FieldMetadata { FieldName = "Name", DataType = typeof(string) }
    }
});

// Set statistics
statsProvider.SetRowCount("CustomObject", 50000);
```

## Database Provider Support

### Postgres
- ✅ Full SOQL support
- ✅ Native RLS policies
- ✅ ILIKE for case-insensitive matching
- ✅ LIMIT/OFFSET
- ✅ Arrays for IN clauses

### SQL Server
- ✅ Full SOQL support
- ✅ OFFSET/FETCH for pagination
- ✅ Workarounds for NULLS FIRST/LAST
- ✅ Table-valued parameters for IN clauses

### Mock
- ✅ In-memory execution for testing
- ✅ No database required
- ✅ Predictable metadata and statistics

## Error Handling

```csharp
try
{
    var result = await engine.ExecuteAsync<Account>(soql);
    
    if (result.Success)
    {
        Console.WriteLine($"Retrieved {result.RecordCount} records in {result.ExecutionTime.TotalMilliseconds}ms");
    }
    else
    {
        Console.WriteLine($"Error: {result.Error}");
    }
}
catch (SoqlParseException ex)
{
    Console.WriteLine($"Invalid SOQL: {ex.Message}");
}
catch (Exception ex)
{
    Console.WriteLine($"Execution error: {ex.Message}");
}
```

## License

MIT License - feel free to use in your projects.

## Contributing

Contributions are welcome! Areas for enhancement:
- Additional database providers (MySQL, Oracle)
- More optimization strategies
- Advanced RLS policies
- Query result transformations
- Performance benchmarks

## Credits

Built with:
- **Dapper** - High-performance object mapper
- **C# Expression Trees** - Compiled result mapping
- **Dynamic Programming** - Join order optimization
- **LRU Caching** - Query plan reuse
