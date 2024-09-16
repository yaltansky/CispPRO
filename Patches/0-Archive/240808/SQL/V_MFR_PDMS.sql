﻿IF OBJECT_ID('V_MFR_PDMS') IS NOT NULL DROP VIEW V_MFR_PDMS
GO
-- SELECT TOP 100 * FROM V_MFR_PDMS
CREATE VIEW V_MFR_PDMS
AS
SELECT 
	X.PDM_ID,
	X.TYPE_ID,
	X.ITEM_ID,
	NAME = CONCAT(P.NAME, CASE WHEN X.VERSION_NUMBER IS NOT NULL THEN ' V.' END, X.VERSION_NUMBER),
    ITEM_NAME = P.NAME,
	X.STATUS_ID,
	STATUS_NAME = S.NAME,
	X.D_DOC,
	X.NUMBER,
	X.VERSION_NUMBER,
	X.MOL_ID,
	MOL_NAME = M.NAME,
	X.EXEC_REGLAMENT_ID,
	X.EXECUTOR_ID,
	X.NOTE,
	X.IS_DELETED,	
	X.UPDATE_DATE
FROM MFR_PDMS X WITH (NOLOCK)
	JOIN MFR_PDM_STATUSES S WITH (NOLOCK) ON S.STATUS_ID = X.STATUS_ID
	LEFT JOIN PRODUCTS P WITH (NOLOCK) ON P.PRODUCT_ID = X.ITEM_ID
	LEFT JOIN MOLS M WITH (NOLOCK) ON M.MOL_ID = X.MOL_ID
GO
