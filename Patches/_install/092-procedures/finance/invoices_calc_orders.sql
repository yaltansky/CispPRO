if object_id('invoices_calc_orders') is not null drop proc invoices_calc_orders
go
-- exec invoices_calc_orders @trace = 1
create proc [invoices_calc_orders]
    @contents app_pkids readonly,
    @trace bit = 0
as
begin

	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	declare @proc_name varchar(50) = object_name(@@procid)
	declare @tid int; exec tracer_init @proc_name, @trace_id = @tid out, @echo = @trace

	declare @tid_msg varchar(max) = concat(@proc_name, '.params:')
	exec tracer_log @tid, @tid_msg

    declare @filter as table(mfr_doc_id int, item_id int, primary key (mfr_doc_id,item_id))
	insert into @filter select distinct mfr_doc_id, item_id from mfr_sdocs_contents c
        join @contents i on i.id = c.content_id

begin

	create table #materials(
		content_row_id int identity primary key,
		content_id int index ix_content,
		due_date datetime,
		mfr_doc_id int,
		item_id int,
		unit_name varchar(30),
		value float		
		)
		create index ix_contents on #materials(mfr_doc_id,item_id)

	create table #invoices(
		inv_row_id int identity primary key,
		inv_id int,
		inv_date datetime,
		mfr_doc_id int,
		item_id int,
		unit_name varchar(30),
		value float
		)
		create index ix_invoices on #invoices(mfr_doc_id,item_id)

	create table #result(		
		content_row_id int index ix_content_row,
		inv_row_id int index ix_inv_row,
		mfr_doc_id int,
		item_id int,
		content_id int index ix_content,
		due_date datetime,
		inv_id int,
		inv_date datetime,
		unit_name varchar(30),		
		plan_q float,
		fact_q float,
		slice varchar(20)
		)

end -- tables

begin
    declare @fid uniqueidentifier set @fid = newid()

	exec tracer_log @tid, 'FIFO'

	-- #materials
		insert into #materials(content_id, due_date, mfr_doc_id, item_id, unit_name, value)
		select 
			x.content_id, dbo.getday(x.opers_to), x.mfr_doc_id, x.item_id, x.unit_name, x.q_brutto_product
		from sdocs_mfr_contents x
            join @filter i on i.mfr_doc_id = x.mfr_doc_id and i.item_id = x.item_id
		where x.is_buy = 1
			and x.q_brutto_product > 0	
			and x.is_deleted = 0
		order by x.mfr_doc_id, x.item_id, x.opers_to

		if @trace = 1 select * into #trace_contents from #materials

	-- #invoices
		insert into #invoices(inv_id, inv_date, mfr_doc_id, item_id, unit_name, value)
		select x.doc_id, d.d_doc, mfr.doc_id, x.product_id, u.name, x.quantity
		from supply_invoices_products x
            join supply_invoices d on d.doc_id = x.doc_id
            join sdocs_mfr mfr on mfr.number = x.mfr_number
                join @filter i on i.mfr_doc_id = mfr.doc_id and i.item_id = x.product_id
            join products_units u on u.unit_id = x.unit_id
		where x.quantity > 0
		order by mfr.doc_id, x.product_id, d.d_doc
		
		if @trace = 1 select * into #trace_jobs from #invoices

	-- FIFO
		insert into #result(
			mfr_doc_id, item_id, content_id, due_date, inv_id, inv_date, unit_name, plan_q, fact_q, slice
            )
		select 
			r.mfr_doc_id, r.item_id, r.content_id, r.due_date,
			p.inv_id, p.inv_date, r.unit_name,
			f.value, f.value, 'mix'
		from #materials r
			join #invoices p on p.mfr_doc_id = r.mfr_doc_id and p.item_id = r.item_id
			cross apply dbo.fifo(@fid, p.inv_row_id, p.value, r.content_row_id, r.value) f
		order by r.content_row_id, p.inv_row_id

    -- left
        insert into #result(mfr_doc_id, item_id, content_id, due_date, unit_name, plan_q, slice)
        select 
            x.mfr_doc_id, x.item_id, x.content_id, x.due_date, x.unit_name,
            f.value, 'left'
        from dbo.fifo_left(@fid) f
            join #materials x on x.content_row_id = f.row_id
        where f.value >= 0.01
    
    -- left (not exists)    
        insert into #result(mfr_doc_id, item_id, content_id, due_date, unit_name, plan_q, slice)
        select 
            x.mfr_doc_id, x.item_id, x.content_id, x.due_date, x.unit_name,
            x.value, 'left'
        from #materials x
        where not exists(select 1 from #result where mfr_doc_id = x.mfr_doc_id and item_id = x.item_id)

    -- right
        insert into #result(mfr_doc_id, item_id, inv_id, inv_date, unit_name, fact_q, slice)
        select x.mfr_doc_id, x.item_id, x.inv_id, x.inv_date, x.unit_name, f.value, 'right'
        from dbo.fifo_right(@fid) f
            join #invoices x on x.inv_row_id = f.row_id
        where f.value >= 0.01

    -- right (not exists)
        insert into #result(mfr_doc_id, item_id, inv_id, inv_date, unit_name, fact_q, slice)
        select x.mfr_doc_id, x.item_id, x.inv_id, x.inv_date, x.unit_name, x.value, 'right'
        from #invoices x
        where not exists(select 1 from #result where mfr_doc_id = x.mfr_doc_id and item_id = x.item_id)

    exec fifo_clear @fid

end -- FIFO

--*******************************************************************
if @trace = 1
	-- контрольная сумма
	select *,	
		check_contents = contents - r_contents,
		check_inv = inv - r_inv
	from (
	select 
		cast((select sum(value) from #materials) as int) as 'contents',
		cast((select sum(plan_q) from #result) as int) as 'r_contents',
		cast((select sum(value) from #invoices) as int) as 'inv',
		cast((select sum(fact_q) from #result) as int) as 'r_inv'
		) u
--*******************************************************************

    delete x from supply_r_provides x
        join @filter i on i.mfr_doc_id = x.mfr_doc_id and i.item_id = x.item_id

    insert into supply_r_provides(
        mfr_doc_id, item_id, content_id, due_date, inv_id, inv_date, unit_name, plan_q, fact_q, slice
        )
    select 
        mfr_doc_id, item_id, content_id, due_date, inv_id, inv_date, unit_name, plan_q, fact_q, slice
    from #result

end
GO
