/****** Object:  Table [BDR_ARTICLES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[BDR_ARTICLES]') AND type in (N'U'))
BEGIN
CREATE TABLE [BDR_ARTICLES](
	[PARENT_ID] [int] NULL,
	[ARTICLE_ID] [int] IDENTITY(1,1) NOT NULL,
	[NAME] [varchar](100) NOT NULL,
	[DIRECTION] [int] NULL DEFAULT ((-1)),
	[LEVEL_ID] [int] NULL,
	[SORT_ID] [float] NULL,
	[IS_DELETED] [bit] NOT NULL DEFAULT ((0)),
	[PATH] [varchar](64) NULL,
	[DESCRIPTION] [varchar](max) NULL,
	[HAS_CHILDS] [bit] NOT NULL DEFAULT ((0)),
	[IS_SOURCE] [bit] NULL,
	[NODE] [hierarchyid] NULL,
	[MAIN_ID] [int] NULL,
	[NODE_ID] [int] NULL,
	[STATUS_ID] [int] NOT NULL DEFAULT ((0)),
	[MOL_ID] [int] NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[SHORT_NAME] [varchar](100) NULL,
	[SUBJECT_ID] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[ARTICLE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_BDR_ARTICLES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[BDR_ARTICLES]') AND name = N'IX_BDR_ARTICLES')
CREATE NONCLUSTERED INDEX [IX_BDR_ARTICLES] ON [BDR_ARTICLES]
(
	[NODE] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Trigger [ti_bdr_articles]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[ti_bdr_articles]'))
EXEC dbo.sp_executesql @statement = N'create trigger [ti_bdr_articles] on [BDR_ARTICLES]
for insert
as
begin

	set nocount on;

	update bdr_articles
	set node_id = article_id,
		direction = -1
	where article_id in (select article_id from inserted)

end' 
GO
/****** Object:  Trigger [tud_bdr_articles_delete]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tud_bdr_articles_delete]'))
EXEC dbo.sp_executesql @statement = N'
create trigger [tud_bdr_articles_delete] on [BDR_ARTICLES]
for update, delete
as
begin

	set nocount on;

	declare @articles table(article_id int)

	if not exists(select 1 from inserted)
	-- delete
		insert into @articles(article_id) select article_id from deleted

	else
	-- update
		insert into @articles(article_id) select article_id from inserted where is_deleted = 1

	if exists(select 1 from findocs where article_id in (select article_id from @articles))
	begin
		rollback
		raiserror(''Нельзя удалить статью, которая используется другими объектами.'', 16, 3)
	end
end
' 
GO
