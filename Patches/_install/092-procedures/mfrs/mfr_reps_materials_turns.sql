if object_id('mfr_reps_materials_turns') is not null drop proc mfr_reps_materials_turns
go
-- exec mfr_reps_materials_turns 1000, @d_from = '2022-06-01'
create proc mfr_reps_materials_turns
	@mol_id int,
	@d_from date = null,
	@d_to date = null,
	@status_id int = null,
	@search varchar(max) = null,
	@product_id int = null,
	@products app_pkids readonly,
	@saveresult bit = 0, -- NOT USED YET
	@checkonly bit = 0,
	@checkresult bit = 0 out,
    @trace bit = 0
as
begin

	set nocount on;

	set @d_from = isnull(@d_from, '1900-01-01')
	set @d_to = isnull(@d_to, dbo.today())
	set @status_id = isnull(@status_id, 100)

	if @checkonly = 0
		exec mfr_items_prices_calc

	create table #products(id int primary key)

	if @search is not null
	begin
		set @search = '%' + replace(@search, ' ', '%') + '%'

		insert into #products
		select p.product_id
		from products p
			left join v_products_groups pg1 on pg1.product_id = p.product_id
			left join v_products_subgroups pg2 on pg2.product_id = p.product_id
		where p.name like @search
			or (pg1.name is not null and pg1.name like @search)
			or (pg2.name is not null and pg2.name like @search)
	end

	else if @product_id is not null
		insert into #products select @product_id
	
	else if exists(select 1 from @products)
		insert into #products select id from @products
	
	declare @filter_products bit = case when exists(select 1 from #products) then 1 end

	-- tables
		create table #data(
			acc_register_id int,
			product_id int index ix_product,
			doc_id int,
			d_doc date,
			number varchar(50),
			unit_name varchar(20),
			quantity float,
			index ix_group(product_id)
		)	

		create table #turn(
			acc_register_id int,
			product_id int index ix_product,
			doc_id int,
			d_doc date,
			number varchar(50),
			unit_name varchar(20),
			q_start float,
			q_input float,
			q_output float,
			q_end float
		)	

	-- подготовка данных
		insert into #data(acc_register_id, product_id, doc_id, d_doc, number, quantity, unit_name)
		select sd.acc_register_id, sp.product_id, sp.doc_id, sd.d_doc, sd.number, tp.direction * sp.quantity, isnull(u.name, '-')
		from sdocs_products sp
			join sdocs sd on sd.doc_id = sp.doc_id
				join sdocs_types tp on tp.type_id = sd.type_id
			left join products_units u on u.unit_id = sp.unit_id
		where tp.direction != 0
			and sd.status_id >= @status_id
			and sd.d_doc <= @d_to
			and sp.quantity > 0 -- фильтруем "кривые" данные
			and (@filter_products is null or sp.product_id in (select id from #products))

	-- единицы измерения
		-- products.unit_id is not null
		update x set 
			quantity = x.quantity * uk.koef,
			unit_name = u.name
		from #data x
			join products p on p.product_id = x.product_id
				join products_units u on u.unit_id = p.unit_id
			join products_ukoefs uk on uk.product_id = x.product_id and uk.unit_from = x.unit_name and uk.unit_to = u.name

		-- products.unit_id is null
		update x set 
			quantity = x.quantity * uk.koef,
			unit_name = pp.unit_name
		from #data x
			join products p on p.product_id = x.product_id and p.unit_id is null
			join (
				select product_id, unit_name = max(unit_name)
				from #data
				group by product_id
			) pp on pp.product_id = x.product_id
			join products_ukoefs uk on uk.product_id = x.product_id and uk.unit_from = x.unit_name and uk.unit_to = pp.unit_name

	-- @checkonly
		if @checkonly = 1
		begin
			if exists(
				select 1 from #data
				group by product_id
				having sum(quantity) < 0
				)
			begin
				select
					product_id,
					@d_to,
					'ИсхОстаток',		
					unit_name,
					q_end = sum(quantity)			
				from #data
				group by product_id, unit_name

				raiserror('В оборотной ведомости есть отрицательный остаток (см. сводный отчёт).', 16, 1)
				set @checkresult = 0
			end
			else 
				set @checkresult = 1
			return
		end

	-- входящий остаток
		insert into #turn(acc_register_id, product_id, d_doc, number, unit_name, q_start)
		select
			acc_register_id,
			product_id,
			@d_from,
			'ВхОстаток',		
			unit_name,
			sum(quantity)			
		from #data x
		where d_doc < @d_from
		group by acc_register_id, product_id, unit_name

	-- обороты
		insert into #turn(acc_register_id, product_id, doc_id, d_doc, number, unit_name, q_input, q_output)
		select
			acc_register_id,
			product_id,
			doc_id,
			d_doc,
			number,
			unit_name,
			case when quantity > 0 then quantity end,
			case when quantity < 0 then -quantity end
		from #data
		where d_doc between @d_from and @d_to

	-- исходящий остаток
		-- if @saveresult = 1
		-- begin
		-- 	delete from mfr_r_materials_lefts where d_doc = @d_to

		-- 	insert into mfr_r_materials_lefts(d_doc, product_id, unit_name, q_left)
		-- 	select
		-- 		@d_to,
		-- 		product_id,
		-- 		max(unit_name),
		-- 		sum(quantity)			
		-- 	from #data
		-- 	group by product_id

		-- 	print 'Исходящий остаток сохранён в регистре MFR_R_MATERIALS_LEFTS.'
		-- 	return
		-- end

		insert into #turn(acc_register_id, product_id, d_doc, number, unit_name, q_end)
		select
			acc_register_id,
			product_id,
			@d_to,
			'ИсхОстаток',		
			unit_name,
			sum(quantity)			
		from #data
		group by acc_register_id, product_id, unit_name

	-- result
		declare @attr_keeper int = (select top 1 attr_id from prodmeta_attrs where code = 'закупка.КодКладовщика')

		select 
			ACC_REGISTER_NAME = ACC.NAME,
			PRODUCT_GROUP1_NAME = PG1.NAME,
			PRODUCT_GROUP2_NAME = PG2.NAME,
			PRODUCT_NAME = P.NAME,
			KEEPER_NAME = pa.ATTR_VALUE,
			X.PRODUCT_ID,
			X.DOC_ID,
			X.D_DOC,
			X.NUMBER,
			UNIT_NAME = LOWER(LTRIM(X.UNIT_NAME)),
			X.Q_START,
			V_START = CAST(NULL AS FLOAT),
			X.Q_INPUT,
			V_INPUT = CAST(NULL AS FLOAT),
			X.Q_OUTPUT,
			V_OUTPUT = CAST(NULL AS FLOAT),
			X.Q_END,
			V_END = CAST(NULL AS FLOAT)
		into #result
		from #turn x
			left join accounts_registers acc on acc.acc_register_id = x.acc_register_id
			join products p on p.product_id = x.product_id
			left join v_products_groups pg1 on pg1.product_id = x.product_id
			left join v_products_subgroups pg2 on pg2.product_id = x.product_id
			left join products_attrs pa on pa.product_id = x.product_id and pa.attr_id = @attr_keeper
		where abs(isnull(x.q_start,0)) > 0.001
			or abs(isnull(x.q_input,0)) > 0.001
			or abs(isnull(x.q_output,0)) > 0.001
			or abs(isnull(x.q_end,0)) > 0.001

		create index ix_join on #result(product_id)

	-- цены
		declare @koef float

		update x set
			@koef = 1.0 / case when x.unit_name = u.name then 1.0 else isnull(uk.koef,1) end,
			v_start = x.q_start * pr.price * @koef,
			v_input = x.q_input * pr.price * @koef,
			v_output = x.q_output * pr.price * @koef,
			v_end = x.q_end * pr.price * @koef
		from #result x
			join mfr_items_prices pr on pr.product_id = x.product_id
				join products_units u on u.unit_id = pr.unit_id
				left join products_ukoefs uk on uk.product_id = pr.product_id and uk.unit_from = u.name and uk.unit_to = x.unit_name

	-- final
		select *,
			PRODUCT_HID = CONCAT('#', PRODUCT_ID)
		from #result

		exec drop_temp_table '#data,#turn,#result'
end
go
