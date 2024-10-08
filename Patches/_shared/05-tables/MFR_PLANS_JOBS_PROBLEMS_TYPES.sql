USE [CISP_SHARED]
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_PROBLEMS_TYPES]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_PLANS_JOBS_PROBLEMS_TYPES](
	[PROBLEM_ID] [int] IDENTITY(1,1) NOT NULL,
	[NAME] [varchar](50) NULL,
	[NOTE] [varchar](max) NULL,
 CONSTRAINT [PK_MFR_PLANS_JOBS_PROBLEMS_TYPES] PRIMARY KEY CLUSTERED 
(
	[PROBLEM_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO
