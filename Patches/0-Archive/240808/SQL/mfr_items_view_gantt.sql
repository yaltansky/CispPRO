if object_id('mfr_items_view_gantt') is not null drop proc mfr_items_view_gantt
go
-- exec mfr_items_view_gantt 651733, 26061 -- CЭЗ
create proc mfr_items_view_gantt
	@doc_id int,
	@product_id int,
	@view_id int = null -- 10 - XML
as
begin

	set nocount on;

    select 
        o.place_id,
        c.item_id,
        name = concat(c.name, ' #', o.number, '-', o.name),
        o.d_from_plan,
        o.d_to_plan,
        o.duration_buffer,
        plan_hours = o.duration_wk * dur.factor / dur_h.factor,
        fact_hours = case when o.fact_q >= o.plan_q then 1 else 0 end * o.duration_wk * dur.factor / dur_h.factor
    into #opers
    from sdocs_mfr_opers o
        join sdocs_mfr_contents c on c.content_id = o.content_id
        join projects_durations dur on dur.duration_id = o.duration_wk_id
        join projects_durations dur_h on dur_h.duration_id = 2
    where c.mfr_doc_id = @doc_id
        and c.product_id = @product_id
        and c.is_buy = 0

    create index ix_opers_place on #opers(place_id, d_from_plan)

	create table #result(
		uid int identity primary key,
        -- tree
		node_id int,
		parent_id int,
		has_childs bit not null default(0),
		name varchar(500),
		node hierarchyid,
		-- attributes
		opers_from date,
		opers_to date,
		opers_days float,
		duration_buffer int,
		progress float,
		)

	insert into #result(node, node_id, has_childs, name, opers_from, opers_to, opers_days, duration_buffer, progress)
	select 
		concat('/', row_number() over (order by name), '/'),
		x.place_id,
		1,
		concat(pl.name, '-', pl.note),
		x.opers_from, 
		x.opers_to,
		datediff(day, x.opers_from, x.opers_to),
		x.duration_buffer,
		isnull(x.fact_hours / nullif(x.plan_hours,0), 0)
	from (
		select 
			place_id,
			opers_from = min(d_from_plan),
			opers_to = max(d_to_plan),
			duration_buffer = min(duration_buffer),
			plan_hours = sum(plan_hours),
			fact_hours = sum(fact_hours)
		from #opers
		group by place_id
		) x
		join mfr_places pl on pl.place_id = x.place_id
	
	insert into #result(node, parent_id, node_id, has_childs, name, opers_from, opers_to, opers_days, duration_buffer, progress)
	select 
		concat(r.node.ToString(), row_number() over (order by o.name), '/'),
		r.node_id,
		o.item_id,
		0,
		o.name,
		o.d_from_plan, o.d_to_plan, datediff(day, o.d_from_plan, o.d_to_plan),
		o.duration_buffer,
		isnull(o.fact_hours / nullif(o.plan_hours,0), 0)
	from #result r
		join #opers o on o.place_id = r.node_id
	
	declare @today datetime = dbo.today()

    if @view_id is null
        select
            x.node_id as 'id',
            x.name as 'text',
            opers_path = '',
            coalesce(x.opers_from, @today) as 'start_date',
            coalesce(x.opers_to, @today + 1) as 'end_date',
            x.opers_days as 'duration',
            x.duration_buffer,
            x.progress,
            cast(row_number() over (order by x.node) as float) as 'sortorder',
            x.parent_id as 'parent',
            'task' as 'type',
            isnull(x.has_childs, 0) as 'open',
            cast(case when x.duration_buffer = 0 then 1 else 0 end as bit) as 'is_critical'
        from #result x
        order by x.node.GetLevel(), x.opers_from, x.opers_to
    
    else if @view_id = 10
    begin
        create table #tasks (
            Id int identity primary key,
            Name varchar(max),
            Summary int,
            Critical bit,
            Start datetime,
            Finish datetime,
            ActualDuration varchar(20),
            Duration varchar(20),
            RemainingDuration varchar(20),
            OutlineLevel int
            )
        insert into #tasks(
            Name, Summary, Critical, [Start], Finish, ActualDuration, Duration, RemainingDuration, OutlineLevel
            )
        select 
            name, has_childs,
            case when duration_buffer = 0 then 1 else 0 end,
            opers_from, opers_to, 
            concat('PT', opers_days * 8, 'H'),
            concat('PT', opers_days * 8, 'H'),
            concat('PT', case when progress >= 1 then 0 else opers_days end * 8, 'H'),
            case when has_childs = 1 then 1 else 2 end
        from #result
        order by node

        declare @result_xml xml = (
            select *
            from (
                select 			
                    14 as 'SaveVersion',
                    concat('Операции заказа #', @doc_id) as 'Title',
                    (select min(Start) from #tasks) as 'StartDate',
                    (
                        select * from #tasks Task order by Id
                        for xml auto, type, elements
                    ) Tasks
                ) Project
            for xml auto, type, elements
            )

        set @result_xml = replace(cast(@result_xml as varchar(max)), '<Project>', '<Project xmlns="http://schemas.microsoft.com/project">')
        select @result_xml
    end
	exec drop_temp_table '#opers,#result'
end
go

-- exec mfr_items_view_gantt 651733, 26061, @view_id = 10 -- CЭЗ