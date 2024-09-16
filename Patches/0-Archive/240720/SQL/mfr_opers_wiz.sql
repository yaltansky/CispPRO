if object_id('mfr_opers_wiz') is not null drop proc mfr_opers_wiz
go
-- exec mfr_opers_wiz 1000, 503, null, 23401, 'wkSheetDetails'
create proc mfr_opers_wiz
	@mol_id int,
    @place_id int,
    @equipment_id int,
    @wk_sheet_id int,
    @part varchar(50)
as
begin
    set nocount on;

    declare @today date = dbo.today()

    -- #executors
    create table #executors(
        ROW_ID INT IDENTITY PRIMARY KEY,
        MOL_ID INT,
        HAS_CHILDS BIT,
        NAME VARCHAR(250),
        POST_NAME VARCHAR(250),
        WK_SHEET_ID INT,
        WK_HOURS FLOAT,
        LEFT_HOURS FLOAT
        )

    -- info
        if @part = 'workers'
            insert into #executors(
                mol_id, name, post_name, left_hours, wk_sheet_id
                )
            select x.mol_id, mols.name, post_name = mp.name, left_hours = x.fact_hours, wd.wk_sheet_id
            from (
                select top 20 je.mol_id, 
                    fact_hours = sum(je.duration_wk * dur.factor / dur_h.factor)
                from mfr_plans_jobs_details jd with(nolock)
                    join mfr_plans_jobs_executors je with(nolock) on je.detail_id = jd.id
                        join projects_durations dur on dur.duration_id = je.duration_wk_id
                        join projects_durations dur_h on dur_h.duration_id = 2
                where je.d_doc between dateadd(m, -3, @today) and @today
                    and resource_id = @equipment_id
                group by je.mol_id
                having sum(je.duration_wk * dur.factor / dur_h.factor) > 1
                ) x
                join mols on mols.mol_id = x.mol_id
                    join mols_posts mp on mp.post_id = mols.post_id
                left join mfr_wk_sheets_details wd with(nolock) on wd.wk_sheet_id = @wk_sheet_id and wd.mol_id = x.mol_id
            order by x.fact_hours desc

        if @part = 'wkSheetDetails' 
        begin
            declare @d_doc date = (select d_doc from mfr_wk_sheets where wk_sheet_id = @wk_sheet_id)

            create table #assigns(mol_id int PRIMARY KEY, plan_hours float)
                insert into #assigns(mol_id, plan_hours)
                select mol_id, plan_hours = sum(e.plan_duration_wk * dur.factor / dur_h.factor)
                from mfr_plans_jobs_executors e
                    join projects_durations dur on dur.duration_id = e.duration_wk_id
                    join projects_durations dur_h on dur_h.duration_id = 2
                where d_doc = @d_doc
                group by mol_id

            insert into #executors(
                mol_id, name, post_name, has_childs, wk_hours, left_hours
                )
            select 
                mol_id, name, post_name, x.has_childs, wk_hours,
                left_hours = wk_hours - isnull(plan_hours, 0)
            from (
                select wd.mol_id, wd.name, post_name = mp.name, wd.has_childs,
                    wk_hours = isnull(wd.brig_wk_hours, wd.wk_hours),
                    e.plan_hours, wd.sort_id
                from mfr_wk_sheets_details wd with(nolock) 
                    join mols_posts mp on mp.post_id = wd.wk_post_id
                    left join #assigns e on e.mol_id = wd.mol_id
                where wk_sheet_id = @wk_sheet_id
                    and wd.parent_id is null
                ) x
            where wk_hours > 0
            order by sort_id, name
        end

    select * from #executors

    exec drop_temp_table '#executors,#assigns'
end
go
