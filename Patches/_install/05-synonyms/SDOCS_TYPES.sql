/****** Object:  Synonym [SDOCS_TYPES]    Script Date: 9/18/2024 3:28:00 PM ******/
IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'SDOCS_TYPES' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [SDOCS_TYPES] FOR [CISP_SHARED].[DBO].[SDOCS_TYPES]
GO
