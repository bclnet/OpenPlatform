namespace Open.Tests;

public class Account {
    public Guid Id { get; set; }
    public string? Name { get; set; }
    public string? Industry { get; set; }
    public decimal? AnnualRevenue { get; set; }
    public Guid OwnerId { get; set; }
    public DateTime CreatedDate { get; set; }
}

public class Contact {
    public Guid Id { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public Guid? AccountId { get; set; }
    public Account? Account { get; set; }
    public Guid OwnerId { get; set; }
    public DateTime CreatedDate { get; set; }
}

public class Opportunity {
    public Guid Id { get; set; }
    public string? Name { get; set; }
    public Guid AccountId { get; set; }
    public string? StageName { get; set; }
    public decimal? Amount { get; set; }
    public DateTime? CloseDate { get; set; }
}
