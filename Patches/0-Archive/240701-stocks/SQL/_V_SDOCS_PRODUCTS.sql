﻿IF OBJECT_ID('V_SDOCS_PRODUCTS') IS NOT NULL DROP VIEW V_SDOCS_PRODUCTS
GO
CREATE VIEW V_SDOCS_PRODUCTS
    WITH SCHEMABINDING AS
SELECT 
	X.DOC_ID,
	XD.DETAIL_ID,
	X.SUBJECT_ID,
	X.TYPE_ID,
	X.STOCK_ID,
	X.D_DOC,
	X.D_DELIVERY,
	X.D_ISSUE,
	X.AGENT_ID,
	XD.PRODUCT_ID,
	XD.UNIT_ID,
	XD.QUANTITY,
	XD.VALUE_CCY,
	XD.VALUE_RUR
FROM DBO.SDOCS X
	JOIN DBO.SDOCS_PRODUCTS XD ON XD.DOC_ID = X.DOC_ID
WHERE X.STATUS_ID >= 0
GO
