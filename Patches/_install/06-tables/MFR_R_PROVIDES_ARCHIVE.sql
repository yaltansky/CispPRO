/****** Object:  Table [MFR_R_PROVIDES_ARCHIVE]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_R_PROVIDES_ARCHIVE]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_R_PROVIDES_ARCHIVE](
	[ROW_ID] [int] IDENTITY(1,1) NOT NULL,
	[MFR_DOC_ID] [int] NOT NULL,
	[MFR_TO_DOC_ID] [int] NULL,
	[ITEM_ID] [int] NOT NULL,
	[UNIT_NAME] [varchar](20) NULL,
	[ID_MFR] [int] NULL,
	[ID_ORDER] [int] NULL,
	[ID_INVOICE] [int] NULL,
	[ID_SHIP] [int] NULL,
	[ID_JOB] [int] NULL,
	[D_MFR] [date] NULL,
	[D_MFR_TO] [date] NULL,
	[D_ORDER] [date] NULL,
	[D_INVOICE] [date] NULL,
	[D_DELIVERY] [date] NULL,
	[D_SHIP] [date] NULL,
	[D_JOB] [date] NULL,
	[Q_MFR] [float] NULL,
	[Q_ORDER] [float] NULL,
	[Q_INVOICE] [float] NULL,
	[Q_SHIP] [float] NULL,
	[Q_LZK] [float] NULL,
	[Q_JOB] [float] NULL,
	[Q_DISTRIB] [float] NULL,
	[PRICE] [float] NULL,
	[PRICE_SHIP] [float] NULL,
	[SLICE] [varchar](16) NULL,
	[XSLICE] [varchar](16) NULL,
	[D_CALC] [datetime] NULL,
	[ACC_REGISTER_ID] [int] NULL,
	[AGENT_ID] [int] NULL,
	[STATUS_ID] [int] NULL,
	[MFR_DOC_ID_INIT] [int] NULL,
	[MFR_DOC_ID_SHIP] [int] NULL,
	[MFR_DOC_ID_INVOICE] [int] NULL,
	[D_RETURN] [date] NULL,
	[ID_RETURN] [int] NULL,
	[Q_RETURN] [float] NULL,
	[ARCHIVE] [bit] NULL,
	[ARCHIVE_DATE] [date] NULL,
	[ARCHIVE_USER] [int] NULL
) ON [PRIMARY]
END
GO

