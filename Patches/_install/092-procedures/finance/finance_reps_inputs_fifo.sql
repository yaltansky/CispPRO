if object_id('finance_reps_inputs_fifo') is not null drop proc finance_reps_inputs_fifo
go
create proc finance_reps_inputs_fifo
	@mol_id int,
	@folder_id int
as
begin

	set nocount on;

-- ids
	create table #buffer (findoc_id int primary key)
	insert into #buffer exec objs_folders_ids @folder_id = @folder_id, @obj_type = 'FD'
	
-- #require
	create table #require(
		row_id int identity primary key,
		deal_id int,
		article_id int,
		value float
		)

	insert into #require(deal_id, article_id, value)
	select
		deal_id, article_id, -sum(value_bds)
	from (
		select 
			db.deal_id, 
			isnull(
				case 
					when d.nds_ratio = 0 then ap.sort_nds0
					else ap.sort_std
				end,
				9999) as sort_id,
			db.article_id,
			a.name,
			db.value_bds
		from deals_budgets db
			join deals d on d.deal_id = db.deal_id
			join bdr_articles a on a.article_id = db.article_id
			left join deals_articles_priority ap on ap.article_short_name = isnull(a.short_name, a.name)
		where db.article_id <> 24
		) x
	group by x.deal_id, x.article_id, x.sort_id, x.name
	order by x.deal_id, x.sort_id, x.name

-- #provide
	create table #provide(row_id int identity, deal_id int, findoc_id int, d_doc datetime, value float)
	insert into #provide(deal_id, findoc_id, d_doc, value) 
	select d.deal_id, f.findoc_id, f.d_doc, f.value_ccy
	from v_findocs f
		join budgets b on b.budget_id = f.budget_id
			join deals d on d.deal_id = b.project_id
				join subjects subj on subj.subject_id = d.vendor_id
	where f.article_id = 24
		and f.findoc_id in (select findoc_id from #buffer)
		and subj.subject_id > 0
	order by d.deal_id, f.d_doc, f.findoc_id

-- fifo
	declare @uid uniqueidentifier set @uid = newid()

	select
		r.deal_id,
		r.article_id,
		sum(f.value) as value
	into #result
	from #require r
		join #provide p on p.deal_id = r.deal_id
		cross apply dbo.fifo(@uid, p.row_id, p.value, r.row_id, r.value) f
	group by
		r.deal_id,
		r.article_id

	exec fifo_clear @uid

-- result 
	select
		MFR_NAME = isnull(d.mfr_name, '???'),
		DIRECTION_NAME = isnull(dir.name, '???'),
		DEAL_NAME = d.number,
		ARTICLE_NAME = a.name,
		VALUE_CCY = cast(sum(r.value) as decimal)
	from #result r
		join deals d on d.deal_id = r.deal_id
			join directions dir on dir.direction_id = d.direction_id
		join bdr_articles a on a.article_id = r.article_id
	group by 
		d.mfr_name, dir.name, d.number, a.name

end
GO
