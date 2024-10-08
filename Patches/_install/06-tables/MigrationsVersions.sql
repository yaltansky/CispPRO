/****** Object:  Table [MigrationsVersions]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MigrationsVersions]') AND type in (N'U'))
BEGIN
CREATE TABLE [MigrationsVersions](
	[VerId] [int] IDENTITY(1,1) NOT NULL,
	[VerName] [varchar](50) NULL,
	[VerDate] [datetime] NULL DEFAULT (getdate()),
	[VerPreview] [varchar](max) NULL,
	[VerDescription] [varchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[VerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

