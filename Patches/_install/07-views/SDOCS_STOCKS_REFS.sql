/****** Object:  View [SDOCS_STOCKS_REFS]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[SDOCS_STOCKS_REFS]'))
EXEC dbo.sp_executesql @statement = N'CREATE VIEW [SDOCS_STOCKS_REFS] AS
    SELECT STOCK_ID, NAME FROM SDOCS_STOCKS' 
GO
