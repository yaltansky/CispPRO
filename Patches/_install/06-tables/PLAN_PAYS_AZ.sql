/****** Object:  Table [PLAN_PAYS_AZ]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PLAN_PAYS_AZ]') AND type in (N'U'))
BEGIN
CREATE TABLE [PLAN_PAYS_AZ](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[PERIOD_ID] [varchar](16) NULL,
	[DIRECTION_ID] [int] NULL,
	[MOL_ID] [int] NULL,
	[VENDOR_ID] [int] NULL,
	[AGENT_ID] [int] NULL,
	[AGENT_NAME] [varchar](250) NULL,
	[DEAL_ID] [int] NULL,
	[D_DOC] [datetime] NULL,
	[VALUE_PLAN] [decimal](18, 2) NULL,
	[VALUE_FACT] [decimal](18, 2) NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[PAY_TYPE_ID] [int] NULL,
	[NOTE] [varchar](max) NULL,
	[SUBJECT_ID] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

