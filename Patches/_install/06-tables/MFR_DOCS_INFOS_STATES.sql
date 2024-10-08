/****** Object:  Table [MFR_DOCS_INFOS_STATES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_DOCS_INFOS_STATES]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_DOCS_INFOS_STATES](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[INFO_ID] [int] NULL,
	[MFR_DOC_ID] [int] NULL,
	[STATE_ID] [int] NULL,
	[NAME] [varchar](50) NULL,
	[D_PLAN] [date] NULL,
	[D_FACT] [date] NULL,
	[NOTE] [varchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [MFR_DOCS_INFOS_STATES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_DOCS_INFOS_STATES]') AND name = N'MFR_DOCS_INFOS_STATES')
CREATE NONCLUSTERED INDEX [MFR_DOCS_INFOS_STATES] ON [MFR_DOCS_INFOS_STATES]
(
	[MFR_DOC_ID] ASC,
	[INFO_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
