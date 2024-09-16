-- dbcontext: CISP_REM | CISP_SNRG
	if db_name() not in ('CISP_REM', 'CISP_SNRG')
		begin
			raiserror('This procedure is implemented only in CISP_REM', 16, 1)
			return
		end
		if object_id('mfr_replicate_materials') is not null drop proc mfr_replicate_materials
go
-- exec mfr_replicate_materials @mol_id = 1000, @trace = 1
create proc mfr_replicate_materials
	@mol_id int = null,	
	@trace bit = 1
as
begin
	set nocount on;

	declare @subjectId int = cast(dbo.app_registry_value('MfrReplSubjectId') as int)

	-- prepare
		set @mol_id = isnull(@mol_id, -25)
		
		declare @proc_name varchar(50) = object_name(@@procid)
		declare @tid int; exec tracer_init @proc_name, @trace_id = @tid out, @echo = @trace

		declare @tid_msg varchar(max) = concat(@proc_name, '.params:', 
			' @mol_id=', @mol_id
			)
		exec tracer_log @tid, @tid_msg

	-- packages
		declare @channels varchar(max) = dbo.app_registry_varchar('MfrReplChannelMaterials')
		create table #packages(PackId int primary key)
			insert into #packages select PackId
			from cisp_gate..packs
			where charindex(ChannelName, @channels) > 0
				and ProcessedOn is null

		if not exists(select 1 from #packages)
		begin
			exec tracer_log @tid, 'Нет пакетов для обработки'
			goto final
		end

		set @tid_msg = concat('Будет обработано ', (select count(*) from #packages), ' пакетов')
		exec tracer_log @tid, @tid_msg

		create table #package(
			PackId int,
			DocId varchar(100),
			primary key (PackId, DocId)
			)

	-- #tables
		create table #sdocs(
			EXTERN_ID VARCHAR(100) PRIMARY KEY,
			DocId VARCHAR(100) INDEX IX_DocId,
			DOC_ID INT,
			SUBJECT_ID INT,
			TYPE_ID INT,
			STATUS_ID INT,
			STOCK_ID INT,
			STOCK_NAME VARCHAR(50),
			D_DOC DATETIME,		
			D_DELIVERY DATETIME,	
			D_ISSUE DATETIME,	
			NUMBER VARCHAR(50),
			AGENT_ID INT,
			AGENT_NAME VARCHAR(250),
			AGENT_INN VARCHAR(50),
			MOL_ID INT,
			MOL_NAME VARCHAR(50),
			CCY_ID CHAR(3),
			VALUE_CCY DECIMAL(18,2),
			VALUE_RUR DECIMAL(18,2),
			NOTE VARCHAR(MAX),
			REPLICATE_DATE DATETIME
		)

		create table #sdocs_products(
			EXTERN_DOC_ID VARCHAR(100) INDEX IX_EXTERN,
			MFR_NUMBER VARCHAR(100),
			MFR_NUMBER_FROM VARCHAR(100),
			--
			EXTERN_PRODUCT_ID VARCHAR(32),		
			PRODUCT_ID INT,
			PRODUCT_NAME VARCHAR(500),
			UNIT_ID INT,
			UNIT_NAME VARCHAR(20),
			QUANTITY FLOAT,
			--
			NDS_RATIO DECIMAL(18,4),
			PRICE FLOAT,
			PRICE_PURE FLOAT,
			VALUE_NDS DECIMAL(18,2),		
			VALUE_PURE DECIMAL(18,2),
			VALUE_RUR DECIMAL(18,2),
			VALUE_CCY DECIMAL(18,2),
			NOTE VARCHAR(MAX)
		)

	-- Счета поставщиков
		exec tracer_log @tid, 'Счета поставщиков'

		delete from #package;
		insert into #package(DocId, PackId)		
			select DocId, max(PackId) from cisp_gate..doc_mfr_supplyinvoiceh
			where PackId in (select PackId from #packages)
			group by DocId

		insert into #sdocs(
			DocId, extern_id, type_id, status_id,
			subject_id, stock_id, stock_name,
			d_doc, d_delivery, d_issue, number,
			agent_name, agent_inn,
			mol_name,
			ccy_id, value_rur, value_ccy,
			note
			)
		select 
			h.DocId,
			concat('8.', h.DocId),
			8, -- Счета поставщиков
			case h.DocStatus
				when 'в работе' then 5
				when 'исполнение' then 10
				when 'удалено' then -1
				else 0
			end,
			h.subject_id,
			stocks.stock_id, h.mfrname,
			isnull(h.SpecDate, h.DocDate), h.PlanDate, h.ReadinessDate,
			isnull(h.SpecNo, h.DocNo),
			h.AgentName, h.AgentINN,
			h.ManagerName,
			'RUR',
			d.SumPcT, d.SumPcT,
			h.DocNote		
		from cisp_gate..doc_mfr_supplyinvoiceh h
			join #package hh on hh.PackId = h.PackId and hh.DocId = h.DocId
			left join sdocs_stocks stocks on stocks.name = h.MfrName
			join (
				select DocId, sum(SumPcT) as SumPcT
				from cisp_gate..doc_mfr_supplyinvoiced
				group by DocId
			) d on d.DocId = h.DocId
			
		set @tid_msg = concat('    Счета поставщиков: ', @@rowcount, ' rows')
		exec tracer_log @tid, @tid_msg

		insert into #sdocs_products(
			extern_doc_id,
			mfr_number,
			extern_product_id, product_name,		
			unit_name,
			quantity, 
			value_pure, value_rur
			)
		select
			concat('8.', d.DocId),
			d.MfrNumber,
			concat(@subjectId, '-', d.ItemId),
			d.ItemName,
			lower(ltrim(rtrim(d.UnitName))),
			d.Q,
			d.SumPc, d.SumPcT
		from cisp_gate..doc_mfr_supplyinvoiced d
			join #package hh on hh.PackId = d.PackId and hh.DocId = d.DocId
			join #sdocs h on h.DocId = d.DocId and h.type_id = 8

	-- Поступления на склад
		exec tracer_log @tid, 'Поступления на склад'

		delete from #package;
		insert into #package(DocId, PackId)		
			select DocId, max(PackId) from cisp_gate..doc_mfr_supplywhouseh
			where PackId in (select PackId from #packages)
			group by DocId

		insert into #sdocs(
			DocId, extern_id, type_id, status_id,
			subject_id, stock_id, stock_name,
			agent_name, agent_inn,
			d_doc, number, ccy_id,
			note
			)
		select
			h.DocId,
			concat('9.', h.DocId),
			9, -- Поступления на склад
			case
				when charindex('удален', h.DocStatus) > 0 then -1
				else 100
			end,			
			h.subject_id,
			stocks.stock_id,
			h.MfrName,
			h.AgentName,
			h.AgentINN,
			h.DocDate,
			h.DocNo,
			'RUR',
			h.DocNote
		from cisp_gate..doc_mfr_supplywhouseh h
			join #package hh on hh.PackId = h.PackId and hh.DocId = h.DocId
			left join sdocs_stocks stocks on stocks.name = h.MfrName
		
		set @tid_msg = concat('    Поступление на склад: ', @@rowcount, ' rows')
		exec tracer_log @tid, @tid_msg

		insert into #sdocs_products(
			extern_doc_id,
			extern_product_id, product_name, unit_name,
			mfr_number,
			quantity, value_pure, value_rur
			)
		select
			concat('9.', d.DocId),
			concat(@subjectId, '-', d.ItemId),
			d.ItemName,
			lower(ltrim(rtrim(d.UnitName))),
			d.MfrNumber,
			d.Q,
			d.SumPc,
			d.SumPcT
		from cisp_gate..doc_mfr_supplywhoused d
			join #package hh on hh.PackId = d.PackId and hh.DocId = d.DocId
			join #sdocs h on h.DocId = d.DocId and h.type_id = 9
	
	-- Выдача в производство
		exec tracer_log @tid, 'Выдача в производство'

		delete from #package;
		insert into #package(DocId, PackId)		
			select DocId, max(PackId) from cisp_gate..doc_mfr_supplyh
			where PackId in (select PackId from #packages)
			group by DocId

		insert into #sdocs(
			DocId, extern_id, type_id, status_id,
			subject_id,
			d_doc, number,
			note
			)
		select
			h.DocId,
			concat('12.', h.DocId),
			12, -- Выдача в производство
			case
				when charindex('удален', h.DocStatus) > 0 then -1
				else 100
			end,			
			h.subject_id,
			h.DocDate,
			h.DocNo,
			h.DocNote
		from cisp_gate..doc_mfr_supplyh h
			join #package hh on hh.PackId = h.PackId and hh.DocId = h.DocId
		
		set @tid_msg = concat('    Выдача в производство: ', @@rowcount, ' rows')
		exec tracer_log @tid, @tid_msg

		insert into #sdocs_products(
			extern_doc_id,
			mfr_number,
			extern_product_id, product_name,
			unit_name,
			quantity, value_pure, value_rur
			)
		select
			concat('12.', d.DocId),
			d.MfrNumber,
			concat(@subjectId, '-', d.ItemId),
			d.ItemName,
			lower(ltrim(rtrim(d.UnitName))),
			d.Q,
			d.SumPc,
			d.SumPcT
		from cisp_gate..doc_mfr_supplyd d
			join #package hh on hh.PackId = d.PackId and hh.DocId = d.DocId
			join #sdocs h on h.DocId = d.DocId and h.type_id = 12
			left join mfr_sdocs mfr on mfr.number = d.mfrnumber

	-- Перераспределение материалов
		exec tracer_log @tid, 'Перераспределение материалов'

		delete from #package;
		insert into #package(DocId, PackId)		
			select DocId, max(PackId) from cisp_gate..doc_mfr_supplyreallocationh
			where PackId in (select PackId from #packages)
			group by DocId

		insert into #sdocs(
			DocId, extern_id, type_id, status_id,
			subject_id,
			d_doc, number,
			note
			)
		select
			h.DocId,
			concat('13.', h.DocId),
			13, -- Перераспределение материалов
			case
				when charindex('удален', h.DocStatus) > 0 then -1
				else 100
			end,			
			h.subject_id,
			h.DocDate,
			h.DocNo,
			h.DocNote
		from cisp_gate..doc_mfr_supplyreallocationh h
			join #package hh on hh.PackId = h.PackId and hh.DocId = h.DocId
					
		set @tid_msg = concat('    Перераспределение материалов: ', @@rowcount, ' rows')
		exec tracer_log @tid, @tid_msg

		insert into #sdocs_products(
			extern_doc_id,
			mfr_number, mfr_number_from,
			extern_product_id, product_name,
			unit_name,
			quantity, value_pure
			)
		select
			concat('13.', d.DocId),
			d.MfrNumberTo, d.MfrNumberFrom,
			concat(@subjectId, '-', d.ItemId),
			d.ItemName,
			lower(ltrim(rtrim(d.UnitName))),
			d.Q,
			d.SumPc
		from cisp_gate..doc_mfr_supplyreallocationd d
			join #package hh on hh.PackId = d.PackId and hh.DocId = d.DocId
			join #sdocs h on h.DocId = d.DocId and h.type_id = 13

	-- calc nds_ratio ... (#sdocs_products)
		declare @value_pure float, @nds_ratio float
		update #sdocs_products
		set @nds_ratio = isnull(cast(value_rur / nullif(value_pure,0) - 1.00 as decimal(10,2)), 0.2),
			@value_pure = value_rur / nullif((1 + @nds_ratio), 0),
			nds_ratio = case when @nds_ratio < 0 then 0 else @nds_ratio end,
			value_pure = @value_pure,
			price = value_rur / nullif(quantity,0),
			price_pure = @value_pure / nullif(quantity,0),
			value_nds = value_rur - @value_pure,
			value_ccy = value_rur
		
		update x set value_rur = sp.value_ccy, value_ccy = sp.value_ccy
		from #sdocs x
			join (
				select extern_doc_id, value_ccy = sum(value_ccy)
				from #sdocs_products 
				group by extern_doc_id
			) sp on sp.extern_doc_id = x.extern_id

	BEGIN TRY
	BEGIN TRANSACTION

		-- dictionaries
			exec tracer_log @tid, 'Dictionaries'
			exec mfr_replicate_materials;2

		exec tracer_log @tid, 'SDOCS'
			update x set doc_id = sd.doc_id
			from #sdocs x
				join sdocs sd on sd.extern_id = x.extern_id

			declare @seed_id int = isnull((select max(doc_id) from sdocs), 0)

			update x
			set doc_id = @seed_id + xx.id
			from #sdocs x
				join (
					select row_number() over (order by d_doc, number) as id, extern_id
					from #sdocs
				) xx on xx.extern_id = x.extern_id
			where x.doc_id is null

			delete x from #sdocs x
				join sdocs xx on xx.doc_id = x.doc_id
			where xx.source_id = 1 -- источник "КИСП" не реплицируется

			delete from sdocs where doc_id in (select doc_id from #sdocs)
			delete from sdocs_products where doc_id in (select doc_id from #sdocs)

			SET IDENTITY_INSERT SDOCS ON;

				insert into sdocs(		
					extern_id, doc_id,
					subject_id, stock_id, type_id, status_id,
					d_doc, d_delivery, d_issue,
					number, agent_id, mol_id,			
					ccy_id, value_ccy, value_rur,
					note,
					add_mol_id,
					replicate_date
					)
				select
					extern_id, doc_id,
					subject_id, stock_id, type_id, status_id,
					d_doc, d_delivery, d_issue,
					ltrim(rtrim(number)), agent_id, mol_id,
					ccy_id, value_ccy, value_rur,
					note,
					@mol_id,
					getdate()
				from #sdocs

				set @tid_msg = concat('    sdocs: ', @@rowcount, ' rows inserted')
				exec tracer_log @tid, @tid_msg
			
			SET IDENTITY_INSERT SDOCS OFF;

			EXEC SYS_SET_TRIGGERS 0

				insert into sdocs_products(
					doc_id,
					mfr_number, mfr_number_from,
					product_id, unit_id, quantity,
					nds_ratio, price, price_pure, value_nds, value_pure, value_rur, value_ccy
					)
				select
					h.doc_id,
					d.mfr_number, d.mfr_number_from,
					d.product_id, d.unit_id, d.quantity,
					d.nds_ratio,
					d.price, d.price_pure,
					d.value_nds, d.value_pure, d.value_rur, d.value_ccy
				from #sdocs_products d
					join #sdocs h on h.extern_id = d.extern_doc_id

			EXEC SYS_SET_TRIGGERS 1

		set @tid_msg = concat('Успешно обработано ', (select count(*) from #packages), ' пакетов')
		exec tracer_log @tid, @tid_msg
		
		update cisp_gate..packs set ProcessedOn = getdate()
		where PackId in (select PackId from #packages)

	COMMIT TRANSACTION
	END TRY	

	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
		declare @err varchar(max) = error_message()
		raiserror (@err, 16, 3)
	END CATCH -- TRANSACTION

	final:
		exec drop_temp_table '#sdocs,#sdocs_products'
		exec drop_temp_table '#packages,#package'

		-- close log	
		exec tracer_close @tid
		if @trace = 1 exec tracer_view @tid
		return

	mbr:
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
		raiserror('manual break', 16, 1)
end
GO
-- helper: авто-справочники
create proc mfr_replicate_materials;2
as
begin
	-- AGENTS
		-- by inn
			insert into agents(name, name_print, inn)
			select distinct agent_name, agent_name, agent_inn
			from #sdocs x
			where len(agent_inn) >= 10
				and not exists(
					select 1 from agents where status_id >= 0 and len(inn) >= 10
					and inn = x.agent_inn
					)

			update x
			set agent_id = a.agent_id
			from #sdocs x
				join (
					select inn, agent_id = min(agent_id) from agents
					where status_id = 1 and len(inn) >= 10
					group by inn
					having count(*) <= 3
				) a on a.inn = x.agent_inn
			where len(x.agent_inn) >= 10

		-- by name		
			insert into agents(name, name_print, inn)
			select distinct agent_name, agent_name, agent_inn
			from #sdocs
			where agent_id is null
				and agent_name not in (select name from agents)
				and agent_name <> '-'

			update x
			set agent_id = isnull(a.main_id, a.agent_id)
			from #sdocs x
				join agents a on a.name = x.agent_name
			where x.agent_id is null

	-- PRODUCTS
		-- TEMP (нормализация дубликатов)
			update x set extern_product_id = xx.extern_product_id
			from #sdocs_products x
				join (
					select product_name, extern_product_id = min(extern_product_id)
					from #sdocs_products
					group by product_name
					having count(distinct extern_product_id) > 1
				) xx on xx.product_name = x.product_name

		-- @products
			declare @products table(extern_id varchar(32), name varchar(500))
			
			insert into @products(extern_id, name)
			select extern_id, name = min(name)
			from (
				select extern_id = extern_product_id, name = min(product_name) from #sdocs_products group by extern_product_id
				) u
			group by extern_id

		-- нормализация имён
			update x set
				name = xx.name
			from mfr_replications_products x
				join @products xx on xx.extern_id = x.extern_id
			where x.name <> xx.name

			update p set p.name = r.name
			from mfr_replications_products r
				join products p on p.product_id = r.product_id
			where r.name <> p.name

		-- auto-insert
			-- пополняем mapping
			insert into mfr_replications_products(extern_id, name)
			select x.extern_id, x.name
			from @products x
			where not exists(select 1 from mfr_replications_products where extern_id = x.extern_id)

			update r set product_id = p.product_id
			from mfr_replications_products r
				join products p on p.name = r.name
			where r.product_id is null

			-- пополняем справочник
			insert into products(name, name_print, status_id)
			select distinct name, name, 5
			from mfr_replications_products x
			where product_id is null

			-- завершаем mapping
			update x set product_id = p.product_id
			from mfr_replications_products x
				join products p on p.name = x.name
			where x.product_id is null

		-- back updates
			update x set product_id = isnull(pp.main_id, pp.product_id)
			from #sdocs_products x
				join mfr_replications_products p on p.extern_id = x.extern_product_id
					join products pp on pp.product_id = p.product_id

	-- UNIT_NAME
		-- auto-insert
			declare @seed_id int = isnull((select max(unit_id) from products_units), 0)
			insert into products_units(unit_id, name)
			select @seed_id + (row_number() over (order by unit_name)), u.unit_name
			from (
				select distinct x.unit_name
				from #sdocs_products x
				where not exists(select 1 from products_units where name = x.unit_name)
				) u

		-- back updates
			update x set unit_id = u.unit_id
			from #sdocs_products x
				join products_units u on u.name = x.unit_name
end
GO

-- update cisp_gate..packs set ProcessedOn = null where packId = 840497
-- exec mfr_replicate_materials @mol_id = 1000, @trace = 1
