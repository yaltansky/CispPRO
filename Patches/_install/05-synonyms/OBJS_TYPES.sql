/****** Object:  Synonym [OBJS_TYPES]    Script Date: 9/18/2024 3:28:00 PM ******/
IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'OBJS_TYPES' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [OBJS_TYPES] FOR [CISP_SHARED].[DBO].[OBJS_TYPES]
GO
