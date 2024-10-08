USE [CISP_SHARED]
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PA_SALARY_TYPES]') AND type in (N'U'))
BEGIN
CREATE TABLE [PA_SALARY_TYPES](
	[SALARY_TYPE_ID] [int] NOT NULL,
	[NAME] [varchar](250) NULL,
 CONSTRAINT [PK_PA_SALARY_TYPES] PRIMARY KEY CLUSTERED 
(
	[SALARY_TYPE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO
