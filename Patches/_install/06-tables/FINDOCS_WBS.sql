/****** Object:  Table [FINDOCS_WBS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[FINDOCS_WBS]') AND type in (N'U'))
BEGIN
CREATE TABLE [FINDOCS_WBS](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[FINDOC_ID] [int] NULL,
	[BUDGET_ID] [int] NULL,
	[ARTICLE_ID] [int] NULL,
	[TASK_ID] [int] NULL,
	[VALUE_BIND] [decimal](18, 2) NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[MOL_ID] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Index [IX_FINDOCS_WBS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[FINDOCS_WBS]') AND name = N'IX_FINDOCS_WBS')
CREATE UNIQUE NONCLUSTERED INDEX [IX_FINDOCS_WBS] ON [FINDOCS_WBS]
(
	[FINDOC_ID] ASC,
	[BUDGET_ID] ASC,
	[ARTICLE_ID] ASC,
	[TASK_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
