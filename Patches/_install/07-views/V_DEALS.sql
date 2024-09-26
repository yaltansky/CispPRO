IF OBJECT_ID('V_DEALS') IS NOT NULL DROP VIEW V_DEALS
GO
CREATE VIEW V_DEALS
WITH SCHEMABINDING
AS 
SELECT
    D.DEAL_ID,
	D.BUDGET_ID,
    D.SUBJECT_ID,
    D.VENDOR_ID,
	D.CUSTOMER_ID,
    D.D_DOC,
    D.NUMBER,
	D.DOGOVOR_NUMBER,
	D.DOGOVOR_DATE,
	D.SPEC_NUMBER,
	D.SPEC_DATE,
	D.CRM_NUMBER,
	D.PAY_CONDITIONS,
	DEAL_NAME = CONCAT(D.NUMBER, ' ', AG.NAME),
	VENDOR_NAME = v.SHORT_NAME,
	BUDGET_NAME = B.NAME,
	AGENT_NAME = AG.NAME,
	DIRECTION_NAME = COALESCE(DIR.SHORT_NAME, DIR.NAME, '-'),
	MOL_NAME = MOLS.NAME,
	D.VALUE_CCY,
    DEAL_HID = CONCAT('#', D.DEAL_ID),
	BUDGET_HID = CONCAT('#', D.BUDGET_ID)
from dbo.deals d
	join dbo.subjects as v on v.subject_id = d.vendor_id
	join dbo.budgets b on b.budget_id = d.budget_id
	join dbo.depts dir on dir.dept_id = isnull(d.direction_id,0)
	join dbo.mols on mols.mol_id = d.manager_id
	join dbo.agents ag on ag.agent_id = d.customer_id
	
GO

CREATE UNIQUE CLUSTERED INDEX IX_V_DEALS ON DBO.V_DEALS(DEAL_ID)
GO
CREATE INDEX IX_V_DEALS2 ON DBO.V_DEALS(BUDGET_ID)
GO
