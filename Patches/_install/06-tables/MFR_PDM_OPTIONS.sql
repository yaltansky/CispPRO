/****** Object:  Table [MFR_PDM_OPTIONS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_PDM_OPTIONS]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_PDM_OPTIONS](
	[PDM_OPTION_ID] [int] IDENTITY(1,1) NOT NULL,
	[EXTERN_ID] [varchar](50) NULL,
	[PDM_ID] [int] NOT NULL,
	[GROUP_NAME] [varchar](50) NULL,
	[NAME] [varchar](255) NULL,
	[NOTE] [varchar](max) NULL,
	[IS_DEFAULT] [bit] NOT NULL DEFAULT(0),
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[IS_DELETED] [bit] NOT NULL DEFAULT(0),
	[RESERVED] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[PDM_OPTION_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_RESERVED]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_PDM_OPTIONS]') AND name = N'IX_RESERVED')
CREATE NONCLUSTERED INDEX [IX_RESERVED] ON [MFR_PDM_OPTIONS]
(
	[RESERVED] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
