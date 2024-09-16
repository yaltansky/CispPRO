if object_id('sdocs_stocks_turns_calc') is not null drop proc sdocs_stocks_turns_calc
go
-- exec sdocs_stocks_turns_calc 1000
create proc sdocs_stocks_turns_calc
	@mol_id int,
    @d_from date = null,
    @d_to date = null,
    @enforce bit = 0
as
begin
    set nocount on;
    
    declare @today date = dbo.today()
    if @d_from is null set @d_from = dateadd(d, -datepart(d, @today) + 1, @today)
    if @d_to is null set @d_to = dateadd(d, -1, dateadd(m, 1, @d_from))

	-- tables
		create table #data(
			product_id int index ix_product,
			acc_register_id int,
            stock_id int,
            addr_id int,
			doc_id int,
            d_doc date,
            number varchar(50),
			unit_from_id int,
			unit_id int,
			quantity float,
			index ix_group(product_id)
		)	

		create table #turn(
			product_id int index ix_product,
			acc_register_id int,
            stock_id int,
            addr_id int,
			doc_id int,
			d_doc date,
			number varchar(50),
			unit_id int,
			q_start float,
			q_input float,
			q_output float,
			q_end float
		)	

	-- подготовка данных
		insert into #data(product_id, acc_register_id, stock_id, addr_id, doc_id, d_doc, number, quantity, unit_from_id, unit_id)
		select 
            sp.product_id,
            sd.acc_register_id, isnull(ad.stock_id, sd.stock_id), spd.stock_addr_id,
            sp.doc_id, sd.d_doc, sd.number, tp.direction * sp.quantity, sp.unit_id, sp.unit_id
		from sdocs_products sp
			join sdocs sd on sd.doc_id = sp.doc_id
				join sdocs_types tp on tp.type_id = sd.type_id
            left join sdocs_products_details spd on spd.detail_id = sp.detail_id
                left join sdocs_stocks_addrs ad on ad.addr_id = spd.stock_addr_id
		where tp.direction != 0
			and sd.status_id >= 0
			and sd.d_doc <= @d_to
			and sp.quantity > 0 -- фильтруем "кривые" данные

	-- единицы измерения
		update x set unit_id = isnull(p.unit_id, pp.unit_id)
		from #data x
			join products p on p.product_id = x.product_id
			join (
				select product_id, unit_id = min(unit_from_id) from #data
				group by product_id
			) pp on pp.product_id = x.product_id

		update x set 
			quantity = x.quantity * uk.koef
        from #data x
        	join products_units u1 on u1.unit_id = x.unit_from_id
			join products_units u2 on u2.unit_id = x.unit_id
			join products_ukoefs uk on uk.product_id = x.product_id and uk.unit_from = u1.name and uk.unit_to = u2.name
    
	-- входящий остаток
		insert into #turn(
            product_id, acc_register_id, stock_id, addr_id, d_doc, number, unit_id, q_start
            )
		select
			product_id,
			acc_register_id,
			stock_id,
			addr_id,
			@d_from,
			'ВхОстаток',		
			unit_id,
			sum(quantity)			
		from #data x
		where d_doc < @d_from
		group by product_id, acc_register_id, stock_id, addr_id, unit_id

	-- обороты
		insert into #turn(
            product_id, acc_register_id, stock_id, addr_id, doc_id, d_doc, number, unit_id, q_input, q_output
            )
		select
			product_id,
			acc_register_id,
			stock_id,
			addr_id,
			doc_id,
			d_doc,
			number,
			unit_id,
			case when quantity > 0 then quantity end,
			case when quantity < 0 then -quantity end
		from #data
		where d_doc between @d_from and @d_to

-- исходящий остаток
		insert into #turn(
            product_id, acc_register_id, stock_id, addr_id, d_doc, number, unit_id, q_end
            )
		select
			product_id,
			acc_register_id,
			stock_id,
			addr_id,
			@d_to,
			'ИсхОстаток',		
			unit_id,
			sum(quantity)			
		from #data
		group by product_id, acc_register_id, stock_id, addr_id, unit_id

-- final
    delete from sdocs_r_stocks_turns where mol_id = @mol_id

    insert into sdocs_r_stocks_turns(mol_id, d_from, d_to, acc_register_id, product_id, stock_id, addr_id, unit_id, q_start, q_input, q_output, q_end)
    select @mol_id, @d_from, @d_to, acc_register_id, product_id, stock_id, addr_id, unit_id, q_start, q_input, q_output, q_end
    from #turn

    exec drop_temp_table '#data,#turn'
end
go
