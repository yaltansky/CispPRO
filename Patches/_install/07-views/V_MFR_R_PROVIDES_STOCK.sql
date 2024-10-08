﻿IF OBJECT_ID('V_MFR_R_PROVIDES_STOCK') IS NOT NULL DROP VIEW V_MFR_R_PROVIDES_STOCK
GO
-- SELECT * FROM V_MFR_R_PROVIDES_STOCK
CREATE VIEW V_MFR_R_PROVIDES_STOCK
AS

SELECT 
    X.ACC_REGISTER_ID,
    ACC_REGISTER_NAME = ISNULL(ACC.NAME, '-'),
    ITEM_ID, ITEM_NAME, UNIT_NAME, Q_SHIP, Q_LZK, Q_JOB, Q_RETURN, Q_LEFT
FROM (
    SELECT 
        ACC_REGISTER_ID,
        ITEM_ID, ITEM_NAME, UNIT_NAME,
        Q_SHIP = SUM(Q_SHIP),
        Q_LZK = SUM(Q_LZK),
        Q_JOB = SUM(Q_JOB),
        Q_RETURN = SUM(Q_RETURN),
        Q_LEFT = SUM(Q_LEFT)
    FROM V_MFR_R_PROVIDES_STOCK_ITEMS
    GROUP BY ACC_REGISTER_ID, ITEM_ID, ITEM_NAME, UNIT_NAME
    ) X
    LEFT JOIN ACCOUNTS_REGISTERS ACC ON ACC.ACC_REGISTER_ID = X.ACC_REGISTER_ID
WHERE 
    ABS(Q_LEFT) > 1E-4 OR Q_LZK > 1E-4

GO
