/****** Object:  Synonym [CALENDAR]    Script Date: 9/18/2024 3:28:00 PM ******/
IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'CALENDAR' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [CALENDAR] FOR [CISP_SHARED].[DBO].[CALENDAR]
GO
