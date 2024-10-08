/****** Object:  Table [MFR_WK_EXECUTORS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_WK_EXECUTORS]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_WK_EXECUTORS](
	[WK_EXECUTOR_ID] [bigint] IDENTITY(1,1) NOT NULL,
	[WK_SHEET_ID] [int] NULL,
	[PLAN_JOB_ID] [int] NULL,
	[JOB_DETAIL_ID] [int] NULL,
	[PLACE_ID] [int] NULL,
	[D_DOC] [date] NULL,
	[WK_SHIFT] [varchar](10) NULL,
	[STATUS_ID] [int] NULL,
	[STATUS_NOTE] [varchar](max) NULL,
	[EXECUTOR_ID] [int] NULL,
	[EXECUTORS_COUNT] [int] NULL,
	[WK_HOURS] [float] NULL,
	[NORM_HOURS] [float] NULL,
	[FACT_HOURS] [float] NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[IS_DELETED] [bit] NOT NULL DEFAULT(0)
PRIMARY KEY CLUSTERED 
(
	[WK_EXECUTOR_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO
