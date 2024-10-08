/****** Object:  Table [MFR_R_MILESTONES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_R_MILESTONES]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_R_MILESTONES](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[VERSION_ID] [int] NULL,
	[MFR_DOC_ID] [int] NULL,
	[PRODUCT_ID] [int] NULL,
	[MILESTONE_ID] [int] NULL,
	[MFR_D_PLAN] [date] NULL,
	[D_PLAN] [date] NULL,
	[D_FACT] [date] NULL,
	[PLAN_Q] [float] NULL,
	[FACT_Q] [float] NULL,
	[D_CALC] [datetime] NOT NULL DEFAULT (getdate()),
	[SLICE] [varchar](10) NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO

/****** Object:  Index [IX]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_R_MILESTONES]') AND name = N'IX')
CREATE NONCLUSTERED INDEX [IX] ON [MFR_R_MILESTONES]
(
	[VERSION_ID] ASC,
	[MFR_DOC_ID] ASC,
	[MILESTONE_ID] ASC,
	[D_PLAN] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_MFR_R_MILESTONES_VERSION_ID]') AND parent_object_id = OBJECT_ID(N'[MFR_R_MILESTONES]'))
ALTER TABLE [MFR_R_MILESTONES]  WITH CHECK ADD  CONSTRAINT [FK_MFR_R_MILESTONES_VERSION_ID] FOREIGN KEY([VERSION_ID])
REFERENCES [MFR_PLANS_VERS] ([VERSION_ID])
ON DELETE CASCADE
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_MFR_R_MILESTONES_VERSION_ID]') AND parent_object_id = OBJECT_ID(N'[MFR_R_MILESTONES]'))
ALTER TABLE [MFR_R_MILESTONES] CHECK CONSTRAINT [FK_MFR_R_MILESTONES_VERSION_ID]
GO
