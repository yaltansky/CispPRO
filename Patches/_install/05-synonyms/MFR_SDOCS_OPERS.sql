/****** Object:  Synonym [MFR_SDOCS_OPERS]    Script Date: 9/18/2024 3:28:00 PM ******/
IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'MFR_SDOCS_OPERS' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [MFR_SDOCS_OPERS] FOR [SDOCS_MFR_OPERS]
GO
