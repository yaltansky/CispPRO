/****** Object:  Table [MFR_R_PLANS_JOBS_ITEMS_0]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_R_PLANS_JOBS_ITEMS_0]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_R_PLANS_JOBS_ITEMS_0](
	[PLAN_ID] [int] NULL,
	[MFR_DOC_ID] [int] NULL,
	[CONTENT_ID] [int] NULL,
	[ITEM_ID] [int] NULL,
	[OPER_NUMBER] [int] NULL,
	[JOB_ID] [int] NULL,
	[JOB_DETAIL_ID] [int] NULL,
	[JOB_DATE] [date] NULL,
	[JOB_STATUS_ID] [int] NULL,
	[PLAN_Q] [float] NULL,
	[FACT_Q] [float] NULL,
	[SLICE] [varchar](20) NULL
) ON [PRIMARY]
END
GO

