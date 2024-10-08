/****** Object:  Table [MFR_ITEMS_BIND_LOG]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_ITEMS_BIND_LOG]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_ITEMS_BIND_LOG](
	[TRAN_ID] [uniqueidentifier] NULL,
	[TRAN_DATE] [datetime] NOT NULL DEFAULT getdate(),
	[MOL_ID] [int] NULL,
	[ACTION] [varchar](50) NULL,
	[CONTENT_ID] [int] NULL,
	[STATUS_ID] [int] NULL,
	[MILESTONE_ID] [int] NULL,
	[MILESTONE_SLICE] [varchar](10) NULL,
	[D_AFTER] [datetime] NULL,
	[D_BEFORE] [datetime] NULL,
	[USE_DRAFT_DATE] [bit] NULL,
	[SUPPLIER_ID] [int] NULL,
	[MANAGER_ID] [int] NULL,
	[PROGRESS] [float] NULL
) ON [PRIMARY]
END
GO
