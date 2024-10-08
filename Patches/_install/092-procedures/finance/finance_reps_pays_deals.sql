if object_id('finance_reps_pays_deals') is not null drop proc finance_reps_pays_deals
go
create proc [finance_reps_pays_deals]
	@mol_id int,
	@folder_id int
as
begin

	set nocount on;

-- ids
	create table #buffer (findoc_id int primary key)
	insert into #buffer exec objs_folders_ids @folder_id = @folder_id, @obj_type = 'FD'

	create table #result (deal_id int, budget_id int, article_id int, value_plan decimal(18,2), value_fact decimal(18,2))
		create index pk_result on #result(deal_id, budget_id, article_id)
		
-- plans
	insert into #result(deal_id, budget_id, article_id, value_plan)
	select x.deal_id, x.budget_id, db.article_id, db.value_bds
	from deals d
		join (
			select distinct b.project_id as deal_id, b.budget_id
			from v_findocs f
				join #buffer buf on buf.findoc_id = f.findoc_id
				join budgets b on b.budget_id = f.budget_id
		) x on x.deal_id = d.deal_id
		join deals_budgets db on db.deal_id = d.deal_id
	where d.vendor_id > 0 -- кроме Перепродажи, Услуги ... (см. select * from subjects where subject_id < 0)

-- facts
	insert into #result(deal_id, budget_id, article_id, value_fact)
	select b.project_id, b.budget_id, f.article_id, f.value_ccy
	from v_findocs f
		join #buffer buf on buf.findoc_id = f.findoc_id	
		join budgets b on b.budget_id = f.budget_id
	
-- result
	select
		MFR_NAME,
		DIRECTION_NAME,
		DEAL_NUMBER,
		ARTICLE_NAME,
		CASHFLOW_NAME,
		VALUE_PLAN = nullif(value_plan, 0),
		VALUE_FACT = nullif(value_fact, 0),
		VALUE_DIFF = nullif(value_plan - value_fact, 0),
		VALUE_DIFFP = value_fact / nullif(value_plan,0)
	from (
		select
			mfr_name,
			direction_name,
			deal_number,
			article_name,
			cashflow_name,
			value_plan = isnull(sum(value_plan),0),
			value_fact = isnull(sum(value_fact),0)
		from (
			select
				mfr_name = d.mfr_name,
				direction_name = dir.name,
				deal_number = d.number,
				article_name = a.name,
				cashflow_name = case when r.value_plan > 0 or r.value_fact > 0 then 'Приходы' else 'Расходы' end,
				value_plan = r.value_plan,
				value_fact = r.value_fact
			from #result r
				left join deals d on d.deal_id = r.deal_id
					left join directions dir on dir.direction_id = d.direction_id
				left join bdr_articles a on a.article_id = r.article_id
			) u
		group by 
			mfr_name,
			direction_name,
			deal_number,
			article_name,
			cashflow_name
		) uu
end
GO
