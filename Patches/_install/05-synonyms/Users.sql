IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'Users' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [Users] FOR [CISP_SHARED]..[Users]
GO
