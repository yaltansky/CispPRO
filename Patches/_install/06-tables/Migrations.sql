IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Migrations') AND type in (N'U'))
BEGIN
CREATE TABLE Migrations(
	Id int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
	PatchName varchar(250),
	AddDate datetime NULL DEFAULT (getdate()),
	Errors varchar(max),
	ModuleName varchar(50),
	TimeProcessed int,
	Results varchar(max),
	ItemName varchar(250),
	CommandText nvarchar(max),
	ProcessDate datetime NULL
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'Migrations') AND name = N'IX_MIGRATIONS')
    CREATE NONCLUSTERED INDEX IX_MIGRATIONS ON Migrations (PatchName, ItemName)
GO
