/****** Object:  View [PRODUCTS_REFS]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[PRODUCTS_REFS]'))
EXEC dbo.sp_executesql @statement = N'CREATE VIEW [PRODUCTS_REFS] AS SELECT PRODUCT_ID, NAME, NAME_PRINT FROM PRODUCTS
' 
GO
