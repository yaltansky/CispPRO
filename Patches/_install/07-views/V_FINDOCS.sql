﻿IF OBJECT_ID('V_FINDOCS') IS NOT NULL DROP VIEW V_FINDOCS
GO

CREATE VIEW V_FINDOCS
WITH SCHEMABINDING
AS

SELECT 
	F.FINDOC_ID,
	FD.ID AS DETAIL_ID,
	F.SUBJECT_ID,
	F.ACCOUNT_ID,
	F.D_DOC,
	F.NUMBER,
	F.AGENT_ID,
	GOAL_ACCOUNT_ID = COALESCE(FD.GOAL_ACCOUNT_ID, F.GOAL_ACCOUNT_ID, 0),
	BUDGET_ID = COALESCE(FD.BUDGET_ID, F.BUDGET_ID, 0),
	ARTICLE_ID = COALESCE(FD.ARTICLE_ID, F.ARTICLE_ID, 0),
	VALUE_CCY = ISNULL(FD.VALUE_CCY, F.VALUE_CCY),
	VALUE_RUR = ISNULL(FD.VALUE_RUR, F.VALUE_RUR),
	NOTE = ISNULL(FD.NOTE, F.NOTE),
	F.FIXED_DETAILS
FROM DBO.FINDOCS F
	LEFT JOIN DBO.FINDOCS_DETAILS FD ON FD.FINDOC_ID = F.FINDOC_ID
GO
