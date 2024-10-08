/****** Object:  Table [MFR_R_MILESTONES_PF]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_R_MILESTONES_PF]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_R_MILESTONES_PF](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[MFR_DOC_ID] [int] NULL,
	[PRODUCT_ID] [int] NULL,
	[MILESTONE_ID] [int] NULL,
	[D_PLAN] [date] NULL,
	[D_FACT] [date] NULL,
	[PLAN_Q] [float] NULL,
	[FACT_Q] [float] NULL,
	[D_CALC] [datetime] NOT NULL DEFAULT (getdate()),
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Index [IX]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_R_MILESTONES_PF]') AND name = N'IX')
CREATE NONCLUSTERED INDEX [IX] ON [MFR_R_MILESTONES_PF]
(
	[MFR_DOC_ID] ASC,
	[MILESTONE_ID] ASC,
	[D_PLAN] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
