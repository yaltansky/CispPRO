/****** Object:  Synonym [MFR_SDOCS_CONTENTS]    Script Date: 9/18/2024 3:28:00 PM ******/
IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'MFR_SDOCS_CONTENTS' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [MFR_SDOCS_CONTENTS] FOR [SDOCS_MFR_CONTENTS]
GO
