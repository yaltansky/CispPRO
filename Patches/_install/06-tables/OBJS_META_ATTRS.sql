/****** Object:  Table [OBJS_META_ATTRS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[OBJS_META_ATTRS]') AND type in (N'U'))
BEGIN
CREATE TABLE [OBJS_META_ATTRS](
	[ATTR_ID] [int] IDENTITY(1,1) NOT NULL,
	[OBJ_TYPE] [varchar](8) NULL,
	[CODE] [varchar](80) NULL,
	[NAME] [varchar](250) NULL,
	[NOTE] [varchar](max) NULL,
	[PARENT_ID] [int] NULL,
	[HAS_CHILDS] [bit] NULL,
	[NODE] [hierarchyid] NULL,
	[LEVEL_ID] [int] NULL,
	[SORT_ID] [float] NULL,
	[IS_DELETED] [bit] NOT NULL DEFAULT ((0)),
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[ATTR_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

