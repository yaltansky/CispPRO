/****** Object:  Table [MFR_WK_SHEETS_JOBS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_WK_SHEETS_JOBS]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_WK_SHEETS_JOBS](
	[WK_SHEET_ID] [int] NULL,
	[PLAN_JOB_ID] [int] NULL,
	[PLACE_ID] [int] NULL,
	[MFR_NUMBER] [varchar](50) NULL,
	[PRODUCT_ID] [int] NULL,
	[ITEM_ID] [int] NULL,
	[OPER_DATE] [datetime] NULL,
	[OPER_NUMBER] [int] NULL,
	[OPER_NAME] [varchar](100) NULL,
	[ID] [int] NULL,
	[DETAIL_ID] [int] NULL,
	[MOL_ID] [int] NULL,
	[D_DOC] [date] NULL,
	[PLAN_DURATION_WK] [float] NULL,
	[PLAN_DURATION_WK_ID] [int] NULL,
	[DURATION_WK] [float] NULL,
	[DURATION_WK_ID] [int] NULL,
	[OPER_NOTE] [varchar](max) NULL,
	[NORM_DURATION_WK] [float] NULL,
	[PLAN_Q] [float] NULL,
	[FACT_Q] [float] NULL,
	[POST_ID] [int] NULL,
	[RATE_PRICE] [float] NULL,
	[PLAN_SALARY] [float] NULL,
	[FACT_SALARY] [float] NULL,
	[EXEC_STATUS_ID] [int] NULL,
	[SALARY_STATUS_ID] [int] NULL,
	[SLICE] [varchar](20) NULL,
	[FACT_DAY_Q] [float] NULL,
	[PLAN_DAY_Q] [float] NULL,
	[WK_SHIFT] [varchar](10) NULL,
	[RESOURCE_ID] [int] NULL
)
END
GO

