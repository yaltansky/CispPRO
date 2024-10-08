/****** Object:  Table [SDOCS_MFR_DRAFTS_OPERS_LOG]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS_OPERS_LOG]') AND type in (N'U'))
BEGIN
CREATE TABLE [SDOCS_MFR_DRAFTS_OPERS_LOG](
	[TRAN_ID] [uniqueidentifier] NULL,
	[TRAN_CALLER] [varchar](max) NULL,
	[TRAN_ACTION] [char](1) NULL,
	[TRAN_USER_ID] [int] NULL,
	[TRAN_DATE] [datetime] NULL DEFAULT getdate(),
	[OPER_ID] [int] NULL,
	[DRAFT_ID] [int] NULL,
	[NUMBER] [int] NULL,
	[DURATION_WK] [float] NULL,
	[DURATION_WK_ID] [int] NULL
)
END
GO
