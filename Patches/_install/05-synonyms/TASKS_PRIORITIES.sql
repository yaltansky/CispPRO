/****** Object:  Synonym [TASKS_PRIORITIES]    Script Date: 9/18/2024 3:28:00 PM ******/
IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'TASKS_PRIORITIES' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [TASKS_PRIORITIES] FOR [CISP_SHARED].[DBO].[TASKS_PRIORITIES]
GO
