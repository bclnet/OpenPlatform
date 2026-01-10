using Open.Soql.Metadata;
using Open.Soql.Query;
using System.Data;

namespace Open.Tests;

/// <summary>
/// Usage examples for the SOQL engine
/// </summary>
public class UsageExamples {
    public async Task BasicQueryExample() {
        // Setup
        IDbConnection? connection = null; // Your database connection
        var metadataProvider = new DatabaseMetadataProvider(connection, DatabaseProvider.Postgres);
        var statsProvider = new DatabaseStatisticsProvider(connection, DatabaseProvider.Postgres);
        var securityProvider = new MockSecurityContextProvider();
        var engine = new SoqlEngine(DatabaseProvider.Postgres, connection, metadataProvider, statsProvider, securityProvider);

        // Execute simple query
        var accounts = await engine.QueryAsync<Account>("SELECT Id, Name, AnnualRevenue FROM Account WHERE Industry = 'Technology' ORDER BY Name LIMIT 10");
        foreach (var account in accounts)
            Console.WriteLine($"{account.Name}: ${account.AnnualRevenue}");
    }

    public async Task AggregateQueryExample() {
        var engine = CreateEngine(null);

        // Execute aggregate query
        var stats = await engine.QueryAsync(@"
                SELECT 
                    StageName, 
                    COUNT(Id) as OpportunityCount,
                    SUM(Amount) as TotalAmount,
                    AVG(Amount) as AverageAmount
                FROM Opportunity 
                WHERE CloseDate >= 2024-01-01
                GROUP BY StageName
                HAVING COUNT(Id) > 5
                ORDER BY TotalAmount DESC
            ");

        foreach (dynamic stat in stats)
            Console.WriteLine($"{stat.StageName}: {stat.OpportunityCount} opportunities, ${stat.TotalAmount:N0}");
    }

    public async Task RelationshipQueryExample() {
        var engine = CreateEngine(null);

        // Query with relationship traversal
        var contacts = await engine.QueryAsync<Contact>(@"
                SELECT 
                    Id, 
                    FirstName, 
                    LastName, 
                    Account.Name,
                    Account.Industry
                FROM Contact 
                WHERE Account.AnnualRevenue > 1000000
                ORDER BY LastName
                LIMIT 50
            ");

        foreach (var contact in contacts)
            Console.WriteLine($"{contact.FirstName} {contact.LastName} - {contact.Account?.Name}");
    }

    public async Task SubQueryExample() {
        var engine = CreateEngine(null);

        // Query with subquery
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
    }

    public async Task ExplainQueryExample() {
        var engine = CreateEngine(null);

        // Get query execution plan
        var plan = await engine.ExplainAsync(@"
                SELECT Id, Name, Account.Name
                FROM Contact
                WHERE CreatedDate > 2024-01-01
                ORDER BY CreatedDate DESC
            ");

        Console.WriteLine($"Estimated Cost: {plan.EstimatedCost}");
        Console.WriteLine($"Base Cardinality: {plan.BaseTableCardinality}");
        Console.WriteLine($"Filtered Cardinality: {plan.FilteredCardinality}");
        Console.WriteLine($"Parallel Execution: {plan.UseParallelExecution}");

        if (plan.OptimizedJoinOrder.Count != 0) {
            Console.WriteLine("Join Order:");
            foreach (var join in plan.OptimizedJoinOrder)
                Console.WriteLine($"  - {join.RelationshipName} ({join.TargetObject})");
        }
    }

    public async Task CacheManagementExample() {
        var engine = CreateEngine(null);

        // Query with caching enabled
        var accounts1 = await engine.QueryAsync("SELECT Id, Name FROM Account LIMIT 100");

        // Second call will use cached plan
        var accounts2 = await engine.QueryAsync("SELECT Id, Name FROM Account LIMIT 100");

        // Invalidate cache when data changes
        engine.InvalidateCache("Account");

        // Clear all caches
        await engine.ClearCachesAsync();

        // Get cache statistics
        var stats = engine.GetCacheStatistics();
        Console.WriteLine($"Total Cache Entries: {stats.TotalEntries}");
        Console.WriteLine($"Total Hits: {stats.TotalHits}");
        Console.WriteLine($"Average Hits: {stats.AverageHits:F2}");
    }

    public async Task PostgresRlsExample() {
        //IDbConnection? connection = null;

        // Apply Postgres native RLS policies
        var rlsProvider = new PostgresRlsProvider();
        var policy = new RlsPolicy {
            Name = "tenant_isolation",
            PolicyType = RlsPolicyType.Custom
        };

        var policySql = rlsProvider.GeneratePolicy("Account", policy, "accounts");

        // Execute policy creation
        // await connection.ExecuteAsync(policySql);

        // Set session context for RLS
        var context = new SecurityContext { UserId = "user-123" };
        var sessionSql = rlsProvider.GetSessionSql(context);

        // await connection.ExecuteAsync(sessionSql);
    }

    static SoqlEngine CreateEngine(IDbConnection? connection) {
        var metadataProvider = new DatabaseMetadataProvider(connection, DatabaseProvider.Postgres);
        var statsProvider = new DatabaseStatisticsProvider(connection, DatabaseProvider.Postgres);
        var securityProvider = new MockSecurityContextProvider();
        return new SoqlEngine(DatabaseProvider.Postgres, connection, metadataProvider, statsProvider, securityProvider, new SoqlEngineOptions {
            EnableRowLevelSecurity = true,
            EnablePlanCaching = true,
            EnableResultCaching = false,
            EnableParallelExecution = true
        });
    }
}
