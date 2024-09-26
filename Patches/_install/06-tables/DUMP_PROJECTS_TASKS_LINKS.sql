/****** Object:  Table [DUMP_PROJECTS_TASKS_LINKS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DUMP_PROJECTS_TASKS_LINKS]') AND type in (N'U'))
BEGIN
CREATE TABLE [DUMP_PROJECTS_TASKS_LINKS](
	[DUMP_ID] [varchar](32) NULL,
	[PROJECT_ID] [int] NULL,
	[SOURCE_ID] [int] NULL,
	[TARGET_ID] [int] NULL,
	[TYPE_ID] [int] NULL
) ON [PRIMARY]
END
GO

