/****** Object:  View [MFR_ATTRS_REFS]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[MFR_ATTRS_REFS]'))
EXEC dbo.sp_executesql @statement = N'CREATE VIEW [MFR_ATTRS_REFS] AS
    SELECT ATTR_ID, NAME FROM PRODMETA_ATTRS X' 
GO
