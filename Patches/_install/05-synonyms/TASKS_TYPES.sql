/****** Object:  Synonym [TASKS_TYPES]    Script Date: 9/18/2024 3:28:00 PM ******/
IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'TASKS_TYPES' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [TASKS_TYPES] FOR [CISP_SHARED].[DBO].[TASKS_TYPES]
GO
