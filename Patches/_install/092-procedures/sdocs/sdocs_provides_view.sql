if object_id('sdocs_provides_view') is not null drop proc sdocs_provides_view
go
-- TODO: revise proc
create proc [sdocs_provides_view]
	@mol_id int,
	@goal_id int = null,
	@group_id varchar(32) = null,
	@node_id int = null,
	@d_from datetime = null,
	@d_to datetime = null,
	@req_doc_id int = null,
	@product_id int = null,
	@slice varchar(50) = null -- 'value_in', 'value_out', 'value_out2', 'value_end', 'value_end2'
as
begin
	
	set nocount on;

	-- declare @ids as app_pkids
	
	-- if @slice = 'value_start'
	-- 	insert into @ids
	-- 	select x.ROW_ID
	-- 	from sdocs_provides x
	-- 	where x.D_MFR < @d_from
	-- 		and (@req_doc_id is null or x.id_mfr = @req_doc_id)
	-- 		and (@product_id is null or x.product_id = @product_id)

	-- else if @slice = 'value_in'
	-- 	insert into @ids
	-- 	SELECT X.ROW_ID
	-- 	from sdocs_provides x
	-- 	where x.d_mfr between @d_from and @d_to
	-- 		and (@req_doc_id is null or x.id_mfr = @req_doc_id)
	-- 		and (@product_id is null or x.product_id = @product_id)
	
	-- else if @slice = 'value_out'
	-- 	insert into @ids
	-- 	select x.row_id
	-- 	from sdocs_provides x
	-- 	where isnull(x.prv_date, @d_from) between @d_from and @d_to
	-- 		and (@req_doc_id is null or x.id_mfr = @req_doc_id)
	-- 		and (@product_id is null or x.product_id = @product_id)

	-- else if @slice = 'value_out2'
	-- 	insert into @ids
	-- 	select x.row_id
	-- 	from sdocs_provides x
	-- 	where x.d_mfr between @d_from and @d_to
	-- 		and isnull(x.prv_date, @d_from - 1) < @d_from
	-- 		and (@req_doc_id is null or x.id_mfr = @req_doc_id)
	-- 		and (@product_id is null or x.product_id = @product_id)

	-- else if @slice = 'value_end'
	-- begin
	-- 	;with nocompleted as (
	-- 		select req_doc_id, product_id
	-- 		from sdocs_provides				
	-- 		group by req_doc_id, product_id
	-- 		having sum(isnull(req_value,0) - isnull(prv_value,0)) > 0
	-- 		)
	-- 		insert into @ids
	-- 		select x.id
	-- 		from sdocs_provides x
	-- 			join nocompleted xx on xx.id_mfr = x.id_mfr and xx.product_id = x.product_id
	-- 		where x.d_mfr <= @d_to
	-- 			and isnull(x.prv_date, @d_to - 1) <= @d_to
	-- 			and (@req_doc_id is null or x.id_mfr = @req_doc_id)
	-- 			and (@product_id is null or x.product_id = @product_id)
	-- end

	-- else if @slice = 'value_end2'
	-- 	insert into @ids
	-- 	select x.id
	-- 	from sdocs_provides x
	-- 	where isnull(x.prv_date, @d_to + 1) > @d_to
	-- 		and (@req_doc_id is null or x.id_mfr = @req_doc_id)
	-- 		and (@product_id is null or x.product_id = @product_id)

	-- declare @rows table(
	-- 	agent_id int,
	-- 	agent_name varchar(250),
	-- 	product_id int,
	-- 	product_name varchar(250),
	-- 	req_doc_id int,
	-- 	req_date datetime,
	-- 	req_number varchar(30),
	-- 	req_value decimal(18,2),
	-- 	prv_doc_id int,
	-- 	prv_date datetime,
	-- 	prv_number varchar(30),
	-- 	prv_value decimal(18,2)
	-- 	)

	-- insert into @rows(
	-- 	agent_id, agent_name, product_id, product_name, req_doc_id, req_date, req_number, req_value, prv_doc_id, prv_date, prv_number, prv_value
	-- 	)
	-- select
	-- 	isnull(ag2.agent_id, ag.agent_id),
	-- 	isnull(ag2.name, ag.name),
	-- 	x.product_id,
	-- 	p.name,
	-- 	x.id_mfr,
	-- 	x.d_mfr,
	-- 	req.number,
	-- 	x.req_value,
	-- 	x.prv_doc_id,
	-- 	x.prv_date,
	-- 	prv.number,
	-- 	x.prv_value
	-- from sdocs_provides x
	-- 	join @ids i on i.id = x.row_id
	-- 	join products p on p.product_id = x.product_id
	-- 	left join sdocs req on req.doc_id = x.id_mfr
	-- 		left join agents ag on ag.agent_id = req.agent_id
	-- 		left join deals d on d.deal_id = req.deal_id
	-- 			left join agents ag2 on ag2.agent_id = d.customer_id
	-- 	left join sdocs prv on prv.doc_id = x.prv_doc_id

	declare @result table(
		node_id int identity primary key,
		parent_id int,
		tmp_node_id varchar(50),
		tmp_parent_id varchar(50),
		name varchar(250),
		node hierarchyid,
		--
		product_id int,
		req_doc_id int,
		req_date datetime,
		req_number varchar(30),
		req_value decimal(18,2),
		prv_doc_id int,
		prv_date datetime,
		prv_number varchar(30),
		prv_value decimal(18,2)
		)

	-- declare @map table(
	-- 	tmp_node_id varchar(30) primary key, node_id int
	-- 	)

	-- -- level 1: agents
	-- insert into @result(tmp_node_id, name)
	-- 	output inserted.tmp_node_id, inserted.node_id into @map
	-- select distinct concat('A',agent_id), agent_name
	-- from @rows
	-- order by agent_name

	-- -- level 2: docs
	-- insert into @result(tmp_parent_id, tmp_node_id, name, req_doc_id)
	-- 	output inserted.tmp_node_id, inserted.node_id into @map
	-- select distinct concat('A', x.agent_id), concat('A', x.agent_id, 'R', req_doc_id), req_number, req_doc_id
	-- from @rows x
	-- order by req_number

	-- -- level 3: products
	-- insert into @result(
	-- 	tmp_parent_id, tmp_node_id, name, product_id,
	-- 	req_doc_id, req_date, req_number, req_value, prv_doc_id, prv_date, prv_number, prv_value
	-- 	)
	-- 	output inserted.tmp_node_id, inserted.node_id into @map
	-- select
	-- 	concat('A', agent_id, 'R', req_doc_id),
	-- 	concat('A', agent_id, 'R', req_doc_id, 'P', product_id, 'PR', prv_doc_id),
	-- 	product_name, product_id,
	-- 	req_doc_id, req_date, req_number, req_value, prv_doc_id, prv_date, prv_number, prv_value
	-- from @rows
	-- order by product_name, req_date, prv_date

	-- update x
	-- set parent_id = m.node_id
	-- from @result x
	-- 	join @map m on m.tmp_node_id = x.tmp_parent_id

	-- -- hierarchy
	-- declare @children tree_nodes
	-- 	insert into @children(node_id, parent_id, num)
	-- 	select node_id, parent_id,  
	-- 		row_number() over (partition by parent_id order by parent_id, node_id)
	-- 	from @result

	-- declare @nodes tree_nodes; insert into @nodes exec tree_calc @children

	-- update x
	-- set node = xx.node
	-- from @result x
	-- 	join @nodes as xx on xx.node_id = x.node_id

	select * from @result order by node
end
GO
