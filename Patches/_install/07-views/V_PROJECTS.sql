﻿IF OBJECT_ID('V_PROJECTS') IS NOT NULL DROP VIEW V_PROJECTS
GO
-- SELECT TOP 10 * FROM V_PROJECTS
CREATE VIEW V_PROJECTS
AS

SELECT
    X.PROJECT_ID,
    X.PARENT_ID,
    X.STATUS_ID,
    X.SUBJECT_ID,
    PARENT_NAME = P2.NAME,
    X.D_FROM,
    X.D_TO_FORECAST,
    PROGRESS_EXEC = CAST(X.PROGRESS_EXEC * 100 AS INT),
    SUBJECT_NAME = SUBJECTS.SHORT_NAME,
    STATUS_NAME = STATUSES.NAME,
    X.TYPE_ID,
    X.NAME,
    CURATOR_NAME = M1.NAME,
    CHIEF_NAME = M2.NAME,
    ADMIN_NAME = M3.NAME,
    X.CURATOR_ID,
    X.CHIEF_ID,
    X.ADMIN_ID,
    PC.CCY_ID,
    PC.VALUE_CCY,
    X.MX_CPI,
    X.MX_SPI,
    X.CONTENT
FROM PROJECTS X 
    LEFT JOIN PROJECTS P2 ON P2.PROJECT_ID = X.PARENT_ID
    LEFT JOIN PROJECTS_CONTRACTS PC ON PC.PROJECT_ID = X.PROJECT_ID
        LEFT JOIN SUBJECTS ON SUBJECTS.SUBJECT_ID = PC.SUBJECT_ID
    JOIN PROJECTS_STATUSES STATUSES ON STATUSES.STATUS_ID = X.STATUS_ID
    LEFT JOIN MOLS M1 ON M1.MOL_ID = X.CURATOR_ID
    LEFT JOIN MOLS M2 ON M2.MOL_ID = X.CHIEF_ID
    LEFT JOIN MOLS M3 ON M3.MOL_ID = X.ADMIN_ID

GO
