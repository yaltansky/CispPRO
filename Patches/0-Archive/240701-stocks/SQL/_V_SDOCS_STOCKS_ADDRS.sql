﻿IF OBJECT_ID('V_SDOCS_STOCKS_ADDRS') IS NOT NULL DROP VIEW V_SDOCS_STOCKS_ADDRS
GO
CREATE VIEW V_SDOCS_STOCKS_ADDRS AS
	SELECT 
		X.ADDR_ID,
        X.STOCK_ID,
		STOCK_NAME = BU.NAME,
		X.NAME,
		X.ADD_DATE,
		X.ADD_MOL_ID,
		X.UPDATE_DATE,
		X.UPDATE_MOL_ID,
		X.IS_DELETED
	FROM SDOCS_STOCKS_ADDRS X
		LEFT JOIN SDOCS_STOCKS BU ON BU.STOCK_ID = X.STOCK_ID
GO
-- SELECT * FROM V_SDOCS_STOCKS_ADDRS

IF OBJECT_ID('SDOCS_STOCKS_ADDRS_REFS') IS NOT NULL DROP VIEW SDOCS_STOCKS_ADDRS_REFS
GO
CREATE VIEW SDOCS_STOCKS_ADDRS_REFS AS
    SELECT ADDR_ID, NAME FROM SDOCS_STOCKS_ADDRS
GO
