/****** Object:  Table [PRODUCTS_GROUPS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PRODUCTS_GROUPS]') AND type in (N'U'))
BEGIN
CREATE TABLE [PRODUCTS_GROUPS](
	[GROUP_ID] [int] IDENTITY(1,1) NOT NULL,
	[LEVEL_ID] [int] NULL,
	[NAME] [varchar](255) NOT NULL,
	[SORT_ID] [float] NULL,
PRIMARY KEY CLUSTERED 
(
	[GROUP_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO

