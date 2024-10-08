/****** Object:  Table [OBJS_SHARES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[OBJS_SHARES]') AND type in (N'U'))
BEGIN
CREATE TABLE [OBJS_SHARES](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[OBJ_UID] [int] NULL,
	[MOL_ID] [int] NULL,
	[MOL_NODE_ID] [int] NULL,
	[RESERVED] [varchar](max) NULL,
	[A_READ] [tinyint] NOT NULL DEFAULT ((1)),
	[A_UPDATE] [tinyint] NOT NULL DEFAULT ((0)),
	[A_ACCESS] [tinyint] NOT NULL DEFAULT ((0)),
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

