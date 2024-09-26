﻿if object_id('mfr_reps_plan_rates') is not null drop proc mfr_reps_plan_rates
go
-- exec mfr_reps_plan_rates 1000, @d_doc_from = '2022-03-01'
create proc mfr_reps_plan_rates
	@mol_id int,	
	@d_doc_from date = null,
	@d_doc_to date = null,
	@version_id int = 0
as
begin

	set @version_id = isnull(
		@version_id,
		(select max(version_id) from mfr_plans_vers) -- последняя версия
		)

	select
		GROUP_NAME = ISNULL(A.NAME, '-'),
		MFR_PRIORITY = MFR.PRIORITY_FINAL,
		R.D_DOC,
		D_DELIVERY = MFR.D_DELIVERY,
		MONTH_PLAN = CONCAT(RIGHT(DATEPART(YEAR, R.D_DOC), 2), '-', RIGHT('00' + CAST(DATEPART(MONTH, R.D_DOC) AS VARCHAR), 2)),
		MFR_NUMBER = MFR.NUMBER,
		MFR.AGENT_NAME,
		ITEM_NAME = P.NAME,
        ITEM_GROUP_NAME = ISNULL(G1.NAME, '-'),
		R.PLAN_Q,
		R.ORDER_Q,
		VALUE_WORK = SP.VALUE_WORK * R.ORDER_Q / NULLIF(SP.QUANTITY,0),
		MFR_DOC_HID = CONCAT('#', R.MFR_DOC_ID)
	from mfr_r_plans_rates r with(nolock)
		join mfr_sdocs mfr with(nolock) on mfr.doc_id = r.mfr_doc_id
			left join sdocs_products sp with(nolock) on sp.doc_id = mfr.doc_id and sp.product_id = r.item_id
		left join prodmeta_attrs a on a.attr_id = r.product_group_id
		left join products p on p.product_id = r.item_id		
        left join mfr_products_grp1 g1 on g1.product_id = r.item_id
	where r.version_id = @version_id
		and (@d_doc_from is null or isnull(r.d_doc, @d_doc_from) >= @d_doc_from)
		and (@d_doc_to is null or isnull(r.d_doc, @d_doc_to) <= @d_doc_to)
		
end
GO
