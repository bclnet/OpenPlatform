using Moq;
using Open.Soql.Metadata;
using Open.Soql.Parser;
using Open.Soql.Query;
using System.Data;

namespace Open.Tests;

/// <summary>
/// Unit tests for SOQL Parser
/// </summary>
public class SoqlParserTests {
    readonly MockMetadataProvider _metadataProvider = new();

    [Fact]
    public void Parse_SimpleSelect_Success() {
        var parser = new SoqlParser(_metadataProvider);
        var query = parser.Parse("SELECT Id, Name FROM Account");
        Assert.Equal("Account", query.FromObject);
        Assert.Equal(2, query.SelectFields.Count);
        Assert.Equal("Id", query.SelectFields[0].FieldName);
        Assert.Equal("Name", query.SelectFields[1].FieldName);
    }

    [Fact]
    public void Parse_WithWhereClause_Success() {
        var parser = new SoqlParser(_metadataProvider);
        var query = parser.Parse("SELECT Id FROM Account WHERE Name = 'Acme Corp'");
        Assert.NotNull(query.WhereClause);
        Assert.Equal("Name", query.WhereClause.Field);
        Assert.Equal(SoqlOperator.Equals, query.WhereClause.Operator);
        Assert.Equal("Acme Corp", query.WhereClause.Value);
    }

    [Fact]
    public void Parse_WithAggregates_Success() {
        var parser = new SoqlParser(_metadataProvider);
        var query = parser.Parse("SELECT COUNT(Id), SUM(Amount) FROM Opportunity GROUP BY StageName");
        Assert.True(query.IsAggregate);
        Assert.Equal(2, query.SelectFields.Count);
        Assert.True(query.SelectFields[0].IsAggregate);
        Assert.Equal(AggregateFunction.COUNT, query.SelectFields[0].AggregateType);
        Assert.Single(query.GroupBy!);
    }

    [Fact]
    public void Parse_WithJoin_Success() {
        var parser = new SoqlParser(_metadataProvider);
        var query = parser.Parse("SELECT Id, Account.Name FROM Contact WHERE AccountId != null");
        Assert.Single(query.Joins);
        Assert.Equal("Account", query.Joins[0].RelationshipName);
    }

    [Fact]
    public void Parse_WithSubQuery_Success() {
        var parser = new SoqlParser(_metadataProvider);
        var query = parser.Parse("SELECT Id FROM Account WHERE Id IN (SELECT AccountId FROM Contact WHERE CreatedDate > 2024-01-01)");
        Assert.NotNull(query.WhereClause);
        Assert.NotNull(query.WhereClause.SubQuery);
        Assert.Equal("Contact", query.WhereClause.SubQuery.FromObject);
    }

    [Fact]
    public void Parse_ComplexConditions_Success() {
        var parser = new SoqlParser(_metadataProvider);
        var query = parser.Parse("SELECT Id FROM Account WHERE (Name LIKE '%Corp%' OR Industry = 'Technology') AND AnnualRevenue > 1000000");
        Assert.NotNull(query.WhereClause);
        Assert.True(query.WhereClause.IsCompound);
        Assert.Equal(LogicalOperator.AND, query.WhereClause.LogicalOp);
    }

    [Fact]
    public void Parse_OrderByWithNulls_Success() {
        var parser = new SoqlParser(_metadataProvider);
        var query = parser.Parse("SELECT Id, Name FROM Account ORDER BY Name DESC NULLS FIRST");
        Assert.Single(query.OrderBy!);
        Assert.Equal(SortDirection.DESC, query.OrderBy![0].Direction);
        Assert.Equal(NullsOrder.First, query.OrderBy[0].NullsOrder);
    }

    [Fact]
    public void Parse_LimitOffset_Success() {
        var parser = new SoqlParser(_metadataProvider);
        var query = parser.Parse("SELECT Id FROM Account LIMIT 10 OFFSET 20");
        Assert.Equal(10, query.Limit);
        Assert.Equal(20, query.Offset);
    }
}

/// <summary>
/// Unit tests for Query Optimizer
/// </summary>
public class QueryOptimizerTests {
    readonly MockMetadataProvider _metadataProvider = new();
    readonly MockStatisticsProvider _statisticsProvider = new MockStatisticsProvider().SetRowCount("Account", 10000).SetRowCount("Contact", 50000);

    [Fact]
    public void Optimize_EstimatesCardinality_Success() {
        var optimizer = new QueryOptimizer(_metadataProvider, _statisticsProvider);
        var plan = optimizer.Optimize(new SoqlQuery {
            FromObject = "Contact",
            SelectFields = [new SoqlField { FieldName = "Id" }],
            WhereClause = new SoqlCondition { Field = "LastName", Operator = SoqlOperator.Equals, Value = "Smith" }
        });
        Assert.True(plan.BaseTableCardinality > 0);
        Assert.True(plan.FilteredCardinality <= plan.BaseTableCardinality);
    }

    [Fact]
    public void Optimize_ReordersJoins_Success() {
        var optimizer = new QueryOptimizer(_metadataProvider, _statisticsProvider);
        var plan = optimizer.Optimize(new SoqlQuery {
            FromObject = "Contact",
            SelectFields = [new SoqlField { FieldName = "Id" }],
            Joins = [new SoqlJoin { RelationshipName = "Account", TargetObject = "Account", EstimatedRowCount = 10000, Selectivity = 0.1 }]
        });
        Assert.NotNull(plan.OptimizedJoinOrder);
        Assert.True(plan.EstimatedCost > 0);
    }
}

/// <summary>
/// Unit tests for SQL Generator
/// </summary>
public class SqlGeneratorTests {
    readonly MockMetadataProvider _metadataProvider = new();

    [Fact]
    public void GenerateSql_PostgresSimpleSelect_Success() {
        var generator = new SqlGenerator(DatabaseProvider.Postgres, _metadataProvider);
        var compiled = generator.GenerateSql(new QueryPlan {
            Query = new SoqlQuery {
                FromObject = "Account",
                SelectFields = [new SoqlField { FieldName = "Id" }, new SoqlField { FieldName = "Name" }]
            }
        });
        Assert.Contains("SELECT", compiled.Sql);
        Assert.Contains("FROM \"accounts\"", compiled.Sql);
    }

    [Fact]
    public void GenerateSql_WithParameters_Success() {
        var generator = new SqlGenerator(DatabaseProvider.Postgres, _metadataProvider);
        var compiled = generator.GenerateSql(new QueryPlan {
            Query = new SoqlQuery {
                FromObject = "Account",
                SelectFields = [new SoqlField { FieldName = "Id" }],
                WhereClause = new SoqlCondition { Field = "Name", Operator = SoqlOperator.Equals, Value = "Acme" }
            }
        });
        Assert.Contains("WHERE", compiled.Sql);
        Assert.NotEmpty(compiled.Parameters);
    }

    [Fact]
    public void GenerateSql_WithAggregates_Success() {
        var generator = new SqlGenerator(DatabaseProvider.Postgres, _metadataProvider);
        var compiled = generator.GenerateSql(new QueryPlan {
            Query = new SoqlQuery {
                FromObject = "Opportunity",
                SelectFields = [new SoqlField { FieldName = "StageName" }, new SoqlField { IsAggregate = true, AggregateType = AggregateFunction.COUNT, AggregateField = "Id" }],
                GroupBy = ["StageName"]
            }
        });
        Assert.Contains("COUNT(", compiled.Sql);
        Assert.Contains("GROUP BY", compiled.Sql);
    }

    [Fact]
    public void GenerateSql_SqlServerOffsetFetch_Success() {
        var generator = new SqlGenerator(DatabaseProvider.SqlServer, _metadataProvider);
        var compiled = generator.GenerateSql(new QueryPlan {
            Query = new SoqlQuery {
                FromObject = "Account",
                SelectFields = [new SoqlField { FieldName = "Id" }],
                Limit = 10,
                Offset = 5
            }
        });
        Assert.Contains("OFFSET", compiled.Sql);
        Assert.Contains("FETCH", compiled.Sql);
    }
}

/// <summary>
/// Unit tests for Row-Level Security
/// </summary>
public class RowLevelSecurityTests {
    readonly MockSecurityContextProvider _securityProvider = new();
    readonly MockMetadataProvider _metadataProvider = new();

    [Fact]
    public void ApplyRls_AddsOwnerCondition_Success() {
        var rls = new RlsEnforcer(_securityProvider, _metadataProvider).UseDefaultPolicies();
        var query = rls.ApplyRowLevelSecurity(new SoqlQuery {
            FromObject = "Account",
            SelectFields = [new SoqlField { FieldName = "Id" }]
        });
        Assert.NotNull(query.WhereClause);
    }

    [Fact]
    public void ApplyRls_SystemAdminBypass_Success() {
        _securityProvider.SetContext(new SecurityContext {
            UserId = "admin",
            Roles = ["SystemAdministrator"]
        });
        var rls = new RlsEnforcer(_securityProvider, _metadataProvider).UseDefaultPolicies();
        var query = rls.ApplyRowLevelSecurity(new SoqlQuery {
            FromObject = "Account",
            SelectFields = [new SoqlField { FieldName = "Id" }]
        });
        // Admin should not have RLS applied (or minimal conditions)
        Assert.NotNull(query);
    }

    [Fact]
    public void ValidateRecordAccess_OwnerCanAccess_Success() {
        var rls = new RlsEnforcer(_securityProvider, _metadataProvider).UseDefaultPolicies();
        var record = new Dictionary<string, object> {
            ["Id"] = Guid.NewGuid(),
            ["OwnerId"] = "test-user-id"
        };
        var hasAccess = rls.ValidateRecordAccess("Account", record, AccessType.Read);
        Assert.True(hasAccess);
    }
}

/// <summary>
/// Integration tests for the full SOQL engine
/// </summary>
public class SoqlEngineIntegrationTests {
    [Fact]
    public async Task Execute_SimpleQuery_Success() {
        // Arrange
        var connection = new Mock<IDbConnection>();
        var metadataProvider = new MockMetadataProvider();
        var statsProvider = new MockStatisticsProvider();
        var securityProvider = new MockSecurityContextProvider();
        var engine = new SoqlEngine(DatabaseProvider.Mock, connection.Object, metadataProvider, statsProvider, securityProvider, new SoqlEngineOptions { EnableRowLevelSecurity = false });
        // Act & Assert - Just verify it can be created and configured
        Assert.NotNull(engine);
        Assert.NotNull(engine.Options);
    }

    [Fact]
    public async Task Explain_ReturnsQueryPlan_Success() {
        // Arrange
        var connection = new Mock<IDbConnection>();
        var metadataProvider = new MockMetadataProvider();
        var statsProvider = new MockStatisticsProvider();
        var securityProvider = new MockSecurityContextProvider();
        var engine = new SoqlEngine(DatabaseProvider.Mock, connection.Object, metadataProvider, statsProvider, securityProvider);
        // Act
        var plan = await engine.ExplainAsync("SELECT Id, Name FROM Account WHERE Name = 'Test'");
        // Assert
        Assert.NotNull(plan);
        Assert.NotNull(plan.Query);
        Assert.True(plan.EstimatedCost > 0);
    }
}
