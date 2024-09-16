if object_id('mfr_items_view') is not null drop proc mfr_items_view
go
-- exec mfr_items_view 1000, '<f><PLAN_ID>0</PLAN_ID><WORK_TYPE_ID>1</WORK_TYPE_ID></f>'
create proc mfr_items_view
	@mol_id int,	
	@filter_xml xml,
	@sort_expression varchar(50) = null,
	@offset int = 0,
	@fetchrows int = 30,
	--
	@rowscount int = null out,
	@trace bit = 0
as
begin
    set nocount on;
	set transaction isolation level read uncommitted;

	-- parse filter
		declare 
			@plan_id int,
			@work_type_id int = 1,
				-- 1 производство
				-- 2 закупка
				-- 3 кооперация
			@doc_id int,
			@type_id int,
			@mfr_status_id int,
			@status_id int,
			@place_id int,
			@milestone_id int,
			@milestone_row_id int,
			@material_id int,
			@resource_id int,
			@attr_name varchar(50),
			@d1_from date, @d1_to date,
			@d2_from date, @d2_to date,
			@d3_from date, @d3_to date,
			@d4_from date, @d4_to date,
			@d5_from date, @d5_to date,
			@manager_id int,
			@extra_id int,
				-- id: 1, name: 'Отставание по началу'
				-- id: 2, name: 'Отставание по завершению'
				-- id: 3, name: 'Опережение по завершению'
				-- ... см.ниже
				-- 32 - search = #manual
				-- 33 - search = #swap
			@filter_attrs bit,
			@folder_id int,
			@buffer_operation int,
				-- 1 add rows to buffer
				-- 2 remove rows from buffer
				-- 99 build distinct PRODUCT_ID in buffer
			@search nvarchar(max)

		declare @handle_xml int; exec sp_xml_preparedocument @handle_xml output, @filter_xml
			select
				@plan_id = plan_id,
				@work_type_id = isnull(nullif(work_type_id,0), 1),
				@doc_id = nullif(doc_id,0),
				@type_id = nullif(type_id,0),
				@mfr_status_id = @filter_xml.value('(/*/MFR_STATUS_ID/text())[1]', 'int'),
				@status_id = @filter_xml.value('(/*/STATUS_ID/text())[1]', 'int'),
				@place_id = nullif(place_id,0),
				@milestone_id = nullif(milestone_id,0),
				@milestone_row_id = nullif(milestone_row_id,0),
				@material_id = nullif(material_id,0),
				@resource_id = nullif(resource_id,0),
				@attr_name = attr_name,
				@d1_from = nullif(d1_from,'1900-01-01'),
				@d1_to = nullif(d1_to,'1900-01-01'),
				@d2_from = nullif(d2_from,'1900-01-01'),
				@d2_to = nullif(d2_to,'1900-01-01'),
				@d3_from = nullif(d3_from,'1900-01-01'),
				@d3_to = nullif(d3_to,'1900-01-01'),
				@d4_from = nullif(d4_from,'1900-01-01'),
				@d4_to = nullif(d4_to,'1900-01-01'),
				@d5_from = nullif(d5_from,'1900-01-01'),
				@d5_to = nullif(d5_to,'1900-01-01'),
				@manager_id = nullif(manager_id,0),
				@extra_id = nullif(extraid,0),
				@filter_attrs = filter_attrs,
				@folder_id = nullif(folder_id,0),
				@buffer_operation = nullif(buffer_operation,0),
				@search = search
			from openxml (@handle_xml, '/*', 2) with (
				PLAN_ID INT,
				WORK_TYPE_ID INT,
				DOC_ID INT,
				TYPE_ID INT,
				PLACE_ID INT,
				MILESTONE_ID INT,
				MILESTONE_ROW_ID INT,
				MATERIAL_ID INT,
				RESOURCE_ID INT,
				ATTR_NAME VARCHAR(50),
				D1_FROM DATE, D1_TO DATE,
				D2_FROM DATE, D2_TO DATE,
				D3_FROM DATE, D3_TO DATE,
				D4_FROM DATE, D4_TO DATE,
				D5_FROM DATE, D5_TO DATE,
				MANAGER_ID INT,
				ExtraId INT,
				FILTER_ATTRS BIT,
				FOLDER_ID INT,
				BUFFER_OPERATION INT,
				Search NVARCHAR(MAX)
				)
		exec sp_xml_removedocument @handle_xml

	-- pattern params
		declare @pkey varchar(50) = 'CONTENT_ID'
		declare @base_view varchar(50) = 'V_SDOCS_MFR_CONTENTS'
		declare @obj_type varchar(16) = 'MFC'
		declare @today datetime = dbo.today()

	-- #ids
		if @folder_id = -1 set @folder_id = dbo.objs_buffer_id(@mol_id)
		create table #ids(id int primary key)
		exec objs_folders_ids @folder_id = @folder_id, @obj_type = @obj_type, @temp_table = '#ids'

	-- @mfr_status_id	
		if @folder_id is not null or @doc_id is not null
			set @mfr_status_id = null

	-- @milestone_row_id
		if @milestone_row_id is not null
		begin
			truncate table #ids
			
			insert into #ids
			select distinct cm.content_id
			from sdocs_mfr_opers o with(nolock)
				join sdocs_mfr_milestones ms with(nolock) on ms.id = @milestone_row_id and ms.doc_id = o.mfr_doc_id and ms.attr_id = o.milestone_id
				join sdocs_mfr_contents c with(nolock) on c.content_id = o.content_id
					join sdocs_mfr_contents cm with(nolock) on cm.mfr_doc_id = c.mfr_doc_id and cm.product_id = c.product_id
						and cm.node.IsDescendantOf(c.node) = 1
						and cm.is_buy = 1
		end

	-- #search_ids
		create table #search_ids(id int primary key)
		declare @search_attr bit = 0

		if @search is not null
		begin
			insert into #search_ids select distinct id from dbo.hashids(@search)
			set @search = replace(replace(@search, '[', ''), ']', '')

			if substring(@search, 1, 1) = ':' begin
				set @search_attr = 1
				set @search = substring(@search, 2, 255)
			end		

			if exists(select 1 from #search_ids) set @search = null
			else set @search = '%' + replace(@search, ' ', '%') + '%'
		end
		
	-- #docs
		create table #docs(id int primary key)
		if len(@search) > 5
		begin
			 insert into #docs select doc_id from mfr_sdocs where number like @search
			 if exists(select 1 from #docs) set @search = null
		end

	-- #plans
		create table #plans(id int primary key)
		if isnull(@plan_id, 0) = 0
			insert into #plans select plan_id from mfr_plans where (status_id = 1 or @doc_id is not null)
		else 
			insert into #plans select @plan_id

        if exists(select 1 from #ids) or exists(select 1 from #search_ids)
        begin
            set @plan_id = null
            delete from #plans
            insert into #plans select plan_id from mfr_plans
        end

	-- @attrs
		declare @attrs as app_pkids
		if @attr_name is not null insert into @attrs select attr_id from mfr_attrs where group_name = @attr_name or name = @attr_name
		
	-- @filter_attrs
		declare @productsQueryTable sysname
		if @filter_attrs = 1 exec products_builder;2 @mol_id, @productsQueryTable out

	-- @extra_id = 4: 'Текущий месяц'
		create table #extra_ids(id int primary key)

		if @extra_id = 4
		begin
			declare @opers_from datetime = dateadd(d, -datepart(d, @today)+1, @today)
			declare @opers_to datetime = dateadd(m, 1, @opers_from) - 1

			select
				content_id, oper_id,
				plan_q = max(plan_q), fact_q = sum(fact_q), d_plan = max(oper_date), d_fact = max(job_date)
			into #jobs_fifo
			from mfr_r_plans_jobs_items
			where plan_id in (select id from #plans)
				and content_id is not null
			group by content_id, oper_id

			update #jobs_fifo set d_fact = null where plan_q > isnull(fact_q,0) and d_fact is not null

			insert into #extra_ids		
			select distinct content_id
			from #jobs_fifo
			where (			
				d_fact between @opers_from and @opers_to -- Факт(до) в периоде и <= заданной даты			
				or (d_plan <= @opers_to and isnull(d_fact, @opers_from) >= @opers_from) -- или (План(до) <= До и (Факт >= От или Пусто))
				)

			drop table #jobs_fifo
		end

		declare @buffer_id int = dbo.objs_buffer_id(@mol_id)

	-- prepare sql
		declare @sql nvarchar(max), @fields nvarchar(max)

		declare @where nvarchar(max) = concat(
			' where (isnull(x.mfr_status_id, 1) != -1) '

			, case isnull(@work_type_id,1)
				when 1 then concat(
                    ' and (x.is_buy = 0)',
                    case when @doc_id is null then 'and (x.mfr_ext_type_id is null)' end -- в деталях прогноз никогда не показываем
                    )
				when 2 then ' and (x.is_buy = 1)'
				when 3 then ' and (x.work_type_3 = 1)'
			end

            , case 
                when isnull(@extra_id, 0) = 100 then ' and (x.mfr_ext_type_id = 1)'
				when @doc_id is null then 'and (x.mfr_ext_type_id is null)'
            end
				
			, case when @doc_id is not null then concat(' and (x.mfr_doc_id = ', @doc_id, ')') end
			, case when @type_id is not null then concat(' and (x.item_type_id = ', @type_id, ')') end
			
			, case 
				when @mfr_status_id = 1000 then 'and exists(select 1 from #viewdocs where id = x.mfr_doc_id)'
				when @mfr_status_id is not null then concat(' and (x.mfr_status_id = ', @mfr_status_id, ')') 
			  end

			, case 
				when @status_id is not null then 
					case
						-- производство
						when @work_type_id = 1 then concat(
                            iif(@place_id is not null,
                                ' and exists(
                                    select 1 from sdocs_mfr_opers with(nolock)
                                    where content_id = x.content_id 
                                        and (@place_id is null or place_id = @place_id)
                                        and (work_type_id = @work_type_id)'
                                , ''
                            )
                            ,
                            case
                                when @status_id = -100 then ' and (status_id between -2 and 99)'
                                else ' and (status_id = @status_id)'
                            end
                            , iif(@place_id is not null, ')', '')
                            )

						-- кооперация
						when @work_type_id = 3 then concat(
                            ' and exists(
                                select 1 from sdocs_mfr_opers with(nolock)
                                where content_id = x.content_id 
                                    and (@place_id is null or place_id = @place_id)
                                    and (work_type_id = @work_type_id)'
                            ,
                            case
                                when @status_id = -100 then ' and (status_id between -2 and 99)'
                                else ' and (status_id = @status_id)'
                            end
                            , ')'
                            )
						
						-- материалы
						when @status_id = -100 then ' and (x.status_id between 0 and 25)'

						else ' 
							and (@place_id is null or place_id = @place_id)
							and (x.status_id = @status_id)
							'
					end
				when @folder_id is not null then '' -- all statuses
				else ' and (x.status_id != -1)'
			end

			, case when @place_id is not null then 
				case
                    -- кооперация
					when @work_type_id = 2 then 
						' and (x.place_id = @place_id
							or exists(
								select 1 from sdocs_mfr_opers with(nolock)
								where content_id = x.content_id and place_id = @place_id
							)
						)'
                    -- сделано
					when @status_id = 100 then 
                        ' and exists(
                            select 1 from sdocs_mfr_opers with(nolock) where content_id = x.content_id and place_id = @place_id and status_id = 100
                            )
                          and not exists(
                            select 1 from sdocs_mfr_opers with(nolock) where content_id = x.content_id and place_id = @place_id and status_id != 100
                            )'
					else ' and exists(select 1 from sdocs_mfr_opers with(nolock) where content_id = x.content_id and place_id = @place_id)' 
				end
			end

			, case when @milestone_id is not null then concat('
                and exists(
					    select 1 from sdocs_mfr_opers with(nolock)
                        where content_id = x.content_id and milestone_id = ', @milestone_id,
                    ')')
			end

			, case when @material_id is not null then concat('
                and exists(
					    select 1 from sdocs_mfr_contents with(nolock) 
                        where mfr_doc_id = x.mfr_doc_id and product_id = x.product_id and x.child_id = parent_id
                            and item_id = ', @material_id,
                    ')')
			end
			
            , case when @resource_id is not null then concat('
                and exists(
					    select 1 from mfr_drafts_opers_resources with(nolock)
                        where draft_id = x.draft_id and resource_id = ', @resource_id,
                    ')')
			end
			
            , case when @attr_name is not null then '
                and exists(
                        select 1 from sdocs_mfr_contents_attrs with(nolock)
                        where content_id = x.content_id and attr_id in (select id from @attrs)
                    )'
			end
			
			-- дата "от"
			, case when @d1_from is not null then concat(' and (x.opers_from >= ''', convert(varchar, @d1_from, 23),  ''')') end
			, case when @d1_to is not null then concat(' and (x.opers_from <= ''', convert(varchar, @d1_to, 23),  ''')') end
			
			-- дата "до"
			, case when @d2_from is not null then concat(' and (x.opers_to >= ''', convert(varchar, @d2_from, 23),  ''')') end
			, case when @d2_to is not null then concat(' and (x.opers_to <= ''', convert(varchar, @d2_to, 23),  ''')') end

			-- дата "От (ПДО)"
			, case when @d4_from is not null then concat(' and (x.opers_from_plan >= ''', convert(varchar, @d4_from, 23),  ''')') end
			, case when @d4_to is not null then concat(' and (x.opers_from_plan <= ''', convert(varchar, @d4_to, 23),  ''')') end

			-- дата "До (ПДО)"
			, case when @d5_from is not null then concat(' and (x.opers_to_plan >= ''', convert(varchar, @d5_from, 23),  ''')') end
			, case when @d5_to is not null then concat(' and (x.opers_to_plan <= ''', convert(varchar, @d5_to, 23),  ''')') end

			-- дата плана
			, case when @d3_from is not null or @d3_to is not null
				then ' and (x.status_id >= 0
						and x.mfr_doc_id in (
							select doc_id from mfr_sdocs with(nolock)
							where status_id >= 0
								and (@d3_from is null or d_issue_plan >= @d3_from)
								and (@d3_to is null or d_issue_plan <= @d3_to)
							)
					)'
			  end

			, case when @manager_id is not null then concat(' and (x.manager_name = ''', 
				(select name from mols where mol_id = @manager_id), ''')'
				) end
			
			, case
				when @search is not null then 
					case
						when @search_attr = 1 then ' and (
							exists(select 1 from sdocs_mfr_drafts_attrs with(nolock) where draft_id = x.draft_id and note like @search)
						)'
						else ' and (
							x.item_name like @search
							or x.name like @search
						)'
					end			
				end
            , case
				-- не показывать без даты ПДО
				when @extra_id is null and @doc_id is null and @folder_id is null then 
                    'and (x.opers_from_plan is not null)'
				-- id: 1, name: 'Отставание по началу'
				when @extra_id = 1 then ' and (x.status_id < 25) and exists(
					select 1 from mfr_sdocs_opers with(nolock) where content_id = x.content_id
						and d_from_plan < @today
						and (@place_id is null or place_id = @place_id)
					)
					'
				-- id: 2, name: 'Отставание по завершению'
				when @extra_id = 2 then ' and (x.status_id < 30) and exists(
					select 1 from mfr_sdocs_opers with(nolock) where content_id = x.content_id
						and status_id != 100
                        and d_to_plan < @today
						and (@place_id is null or place_id = @place_id)
					)
					'
				-- id: 3, name: 'Опережение по завершению'
				when @extra_id = 3 then ' and exists(
					select 1 from mfr_sdocs_opers with(nolock) where content_id = x.content_id
						and status_id = 100
						and d_to_plan > @today
						and (@place_id is null or place_id = @place_id)
					)
					'
				-- id: 4, name: 'Текущий месяц'
				when @extra_id = 4 then ' and (exists(select 1 from #extra_ids where id = x.content_id) and x.status_id != 100)'
				-- id: 5, name: 'Критический путь'
				when @extra_id = 5 then ' and (x.duration_buffer_predict = 0)'
				-- id: 6, name: 'Нет операций'
				when @extra_id = 6 then ' and (isnull(x.opers_count,0) = 0)'
				-- id: 7, name: 'Частичное выполнение'
				when @extra_id = 7 then ' 
					and exists(select 1 from mfr_sdocs_opers with(nolock) where content_id = x.content_id and status_id = 100)
					and exists(select 1 from mfr_sdocs_opers with(nolock) where content_id = x.content_id and status_id < 100)
					'
				-- id: 8, name: 'Нет ПДО'
				when @extra_id = 8 then ' and (x.opers_from_plan is null)'
				-- id: 9, name: 'Нет поставщика'
				when @extra_id = 9 then ' and (x.supplier_name is null)'
				-- id: 10, name: 'Нет менеджера закупок'
				when @extra_id = 10 then ' and (x.manager_name is null)'
				-- id: 11, name: 'Нет тарифов'
				when @extra_id = 11 then ' and exists(
					select 1
					from mfr_drafts_opers o
					where o.draft_id = x.draft_id
						and duration_wk > 0
						and (
							not exists(select 1 from mfr_drafts_opers_executors where oper_id = o.oper_id)
							or exists(
								select 1 from mfr_drafts_opers_executors where oper_id = o.oper_id
									and (isnull(rate_price,0) = 0 or post_id is null)
								)
							)
					)'
				-- id: 12, name: 'Нет количества'
				when @extra_id = 12 then ' and (isnull(x.q_brutto_product,0) = 0)'
				-- id: 13, name: 'Нет участка'
				when @extra_id = 13 then ' and (x.place_id is null)'
				-- id: 14, name: 'Нет заявки'
				when @extra_id = 14 then ' and not exists(select 1 from mfr_r_provides where id_mfr = x.content_id and id_order is not null)'
				-- id: 14, name: 'Нет счёта'
				when @extra_id = 15 then ' and not exists(select 1 from mfr_r_provides where id_mfr = x.content_id and id_invoice is not null)'

				-- id: 21, name: 'Можно выдать'
				when @extra_id = 21 then ' and (
					x.status_id != 100
					and (x.q_provided / nullif(x.q_brutto_product,0) < 0.999)
					and (x.q_provided_max / nullif(x.q_brutto_product,0) >= 0.999)
					)'

				-- id: 22, name: 'Частичная выдача'
				when @extra_id = 22 then ' and (
					x.status_id != 100
					and (x.q_provided > 0)
					and (x.q_provided / nullif(x.q_brutto_product,0) < 0.999)
					)'

				-- id: 32, name: 'Ручная правка'
				when @extra_id = 32 then ' and (x.is_manual_progress = 1)'
				-- id: 33, name: 'Подтверждение потребности'
				when @extra_id = 33 then ' and (x.cancel_reason_id = 20)'
				-- id: 34, name: 'Отмена потребности'
				when @extra_id = 34 then ' and (x.cancel_reason_id in (1,2))'
				-- id: 35, name: 'Замена материала'
				when @extra_id = 35 then ' and (x.is_swap = 1)'
			end
			, case when @filter_attrs = 1 then concat(' and x.item_id in (select product_id from ', @productsQueryTable, ')') end
			)
				
		declare @fields_base nvarchar(max) = N'		
			@mol_id int,
			@today date,
			@place_id int,
			@status_id int,
			@work_type_id int,
			@search nvarchar(max),
			@attrs app_pkids readonly
		'
		declare @join nvarchar(max) = N''
			+ case when @doc_id is null then ' join #plans pl on pl.id = x.plan_id' else '' end
			+ case when @folder_id is not null or exists(select 1 from #ids) then ' join #ids i on i.id = x.content_id ' else '' end
			+ case when exists(select 1 from #search_ids) then ' join #search_ids i2 on i2.id = x.content_id' else '' end
			+ case when exists(select 1 from #docs) then ' join #docs i3 on i3.id = x.mfr_doc_id' else '' end
			
		DECLARE @hint VARCHAR(50) = ' OPTION (RECOMPILE, OPTIMIZE FOR UNKNOWN)'

		if @buffer_operation is null
		begin
			-- @rowscount
			set @sql = N'select @rowscount = count(*) from [base_view] x with(nolock) ' + @join + @where
			set @fields = @fields_base + ', @rowscount int out'

			set @sql = replace(replace(replace(@sql, '[pkey]', @pkey), '[base_view]', @base_view), '[obj_type]', @obj_type)
			set @sql = @sql + @hint

			exec sp_executesql @sql, @fields,
				@mol_id, @today,
				@place_id, @status_id, @work_type_id, @search,
				@attrs,
				@rowscount out
		
			-- @order_by
			declare @order_by nvarchar(150) = N' order by x.opers_to_plan, x.content_id'
			if @sort_expression like '%mfr_priority%' set @order_by = N' order by x.mfr_priority, x.opers_to_plan'
			else if @sort_expression is not null set @order_by = N' order by ' + @sort_expression

			declare @subquery nvarchar(max) = N'(select x.* from [base_view] x with(nolock) '
				+ @join + @where
				+ ') x ' + @order_by

			-- @sql
			set @sql = N'select x.* from ' + @subquery
			-- optimize on fetch
			if @rowscount > @fetchrows set @sql = @sql + ' offset @offset rows fetch next @fetchrows rows only'
			-- add hint
			set @sql = @sql + @hint

			set @fields = @fields_base + ', @offset int, @fetchrows int'

			set @sql = replace(replace(replace(@sql, '[pkey]', @pkey), '[base_view]', @base_view), '[obj_type]', @obj_type)
			if @trace = 1 print @sql

			exec sp_executesql @sql, @fields,
				@mol_id, @today,
				@place_id, @status_id, @work_type_id, @search,
				@attrs,
				@offset, @fetchrows

		end

		else begin
			set @rowscount = -1 -- dummy
			set @fields = @fields_base
			
			declare @bufop int = @buffer_operation
			declare @allproducts bit = 0

			if @buffer_operation = 99
			begin
				-- @rowscount
				set @sql = N'select @rowscount = count(*) from [base_view] x with(nolock) ' + @join + @where + @hint
				declare @fields_count nvarchar(max) = @fields_base + ', @rowscount int out'

				set @sql = replace(replace(replace(@sql, '[pkey]', @pkey), '[base_view]', @base_view), '[obj_type]', @obj_type)
				exec sp_executesql @sql, @fields_count,
					@mol_id, @today,
					@place_id, @status_id, @work_type_id, @search,
					@attrs,
					@rowscount out

				if @rowscount > 10000
				begin
					print concat('use @allproducts = 1 because of rowscount = ', @rowscount)
					set @allproducts = 1
					set @bufop = null
				end
				else begin
					set @bufop = 1
					set @obj_type = @obj_type + '-P' -- virtual obj_type (used in products_builder)
				end
			end

			if @bufop is not null
			begin
				exec objs_buffer_viewhelper
					@buffer_operation = @bufop, @obj_type = @obj_type, @base_view = @base_view, @pkey = @pkey, @join = @join, @where = @where,
					@fields = @fields out, @sql = @sql out			

				if @trace = 1 print 'build buffer: ' + @sql

				exec sp_executesql @sql, @fields,
					@mol_id, @today,
					@place_id, @status_id, @work_type_id, @search,
					@attrs,
					@buffer_id
			end

			if @buffer_operation = 99
				exec products_builder @mol_id = @mol_id,
					@source_name = @base_view,
					@source_key = @pkey,
					@item_name = 'ITEM_ID',
					@obj_type = @obj_type,
					@allproducts = @allproducts

		end -- buffer_operation

	exec drop_temp_table '#plans,#ids,#search_ids,#extra_ids,#docs,#viewdocs'
end
go
