if object_id('mfr_wk_sheets_details_calc') is not null drop proc mfr_wk_sheets_details_calc
go
-- exec mfr_wk_sheets_details_calc
create proc mfr_wk_sheets_details_calc
    @trace bit = 0
as
begin
    -- open log
        set nocount on;
        set transaction isolation level read uncommitted;

        declare @proc_name varchar(50) = object_name(@@procid)
        declare @tid int; exec tracer_init @proc_name, @trace_id = @tid out, @echo = @trace

    -- wk_post_id
        update x set wk_post_id = mols.post_id
        from mfr_wk_sheets_details x
            join mfr_wk_sheets w on w.wk_sheet_id = x.wk_sheet_id
            join mols on mols.mol_id = x.mol_id
        where x.wk_post_id is null
            and w.status_id between 0 and 99

    -- wk_completion
        update x set wk_completion = nullif(
            case
                when j.fact_day_q > j.plan_day_q then 1
                when j.fact_day_q <= j.plan_day_q then j.fact_day_q / nullif(j.plan_day_q,0)
                else 0
            end, 0)
        from mfr_wk_sheets_details x
            join mfr_wk_sheets w on w.wk_sheet_id = x.wk_sheet_id
            left join (
                select wk_sheet_id, mol_id,
                    plan_day_q = isnull(sum(plan_day_q), 0),
                    fact_day_q = isnull(sum(fact_day_q), 0)
                from mfr_wk_sheets_jobs
                group by wk_sheet_id, mol_id
            ) j on j.wk_sheet_id = x.wk_sheet_id and j.mol_id = x.mol_id
        where x.parent_id is null
            and w.status_id between 0 and 99

    -- wk_completion от бригадира
        update x set
            wk_completion = xp.wk_completion
        from mfr_wk_sheets_details x
            join mfr_wk_sheets w on w.wk_sheet_id = x.wk_sheet_id
            join mfr_wk_sheets_details xp on xp.wk_sheet_id = x.wk_sheet_id and xp.id = x.parent_id
        where w.status_id between 0 and 99

    -- status_id
        update mfr_wk_sheets set status_id = 100 
        where status_id between 0 and 99
            and d_doc < dateadd(d, -7, dbo.today())

	-- close log
        exec tracer_close @tid
        if @trace = 1 exec tracer_view @tid
end
go

