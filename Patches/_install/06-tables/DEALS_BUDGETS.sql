/****** Object:  Table [DEALS_BUDGETS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DEALS_BUDGETS]') AND type in (N'U'))
BEGIN
CREATE TABLE [DEALS_BUDGETS](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DEAL_ID] [int] NOT NULL,
	[TASK_ID] [int] NOT NULL,
	[TASK_NAME] [varchar](500) NULL,
	[TASK_DATE] [datetime] NULL,
	[DATE_LAG] [int] NOT NULL DEFAULT ((0)),
	[TYPE_ID] [int] NULL,
	[ARTICLE_ID] [int] NOT NULL,
	[NDS_RATIO] [decimal](18, 2) NULL DEFAULT ((0.18)),
	[RATIO] [decimal](18, 2) NULL,
	[VALUE_BDR] [decimal](18, 2) NULL,
	[VALUE_NDS] [decimal](18, 2) NULL,
	[VALUE_BDS] [decimal](18, 2) NULL,
	[RUNNING_BDS] [decimal](18, 2) NULL,
	[NOTE] [varchar](250) NULL,
	[IS_AUTOMAP] [bit] NOT NULL DEFAULT ((1)),
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_DEALS_BUDGETS]') AND parent_object_id = OBJECT_ID(N'[DEALS_BUDGETS]'))
ALTER TABLE [DEALS_BUDGETS]  WITH CHECK ADD  CONSTRAINT [FK_DEALS_BUDGETS] FOREIGN KEY([DEAL_ID])
REFERENCES [DEALS] ([DEAL_ID])
ON DELETE CASCADE
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_DEALS_BUDGETS]') AND parent_object_id = OBJECT_ID(N'[DEALS_BUDGETS]'))
ALTER TABLE [DEALS_BUDGETS] CHECK CONSTRAINT [FK_DEALS_BUDGETS]
GO
