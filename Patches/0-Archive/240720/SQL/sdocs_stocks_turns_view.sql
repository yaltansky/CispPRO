﻿if object_id('sdocs_stocks_turns_view') is not null drop proc sdocs_stocks_turns_view
go
-- exec sdocs_stocks_turns_view 1000, '<f></f>'
create proc sdocs_stocks_turns_view
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
			@search nvarchar(max),
            @acc_register_id int,
            @stock_id int,
            @d_from date,
            @d_to date,
            @folder_id int,
            @buffer_operation int

		declare @handle_xml int; exec sp_xml_preparedocument @handle_xml output, @filter_xml
			select
				@search = search,
				@acc_register_id = nullif(acc_register_id,0),
				@stock_id = nullif(stock_id,0),
				@d_from = nullif(d_from, '1900-01-01'),
				@d_to = nullif(d_to, '1900-01-01')
			from openxml (@handle_xml, '/*', 2) with (
				Search NVARCHAR(MAX),
				ACC_REGISTER_ID INT,
				STOCK_ID INT,
				D_FROM DATE,
				D_TO DATE
				)
		exec sp_xml_removedocument @handle_xml

    -- auto-recalc
        declare @calc_d_from date, @calc_d_to date
            select @calc_d_from = d_from, @calc_d_to = d_to
            from sdocs_r_stocks_turns where mol_id = @mol_id

        if isnull(@calc_d_from, '1900-01-01') != @d_from or isnull(@calc_d_to, '1900-01-01') != @d_to
            exec sdocs_stocks_turns_calc @mol_id = @mol_id, @d_from = @d_from, @d_to = @d_to

	-- pattern params
		declare @pkey varchar(50) = 'PRODUCT_ID'
		declare @base_view varchar(50) = 'V_SDOCS_R_STOCKS_TURNS_PRODUCTS'
		declare @obj_type varchar(16) = 'P'
		declare @today datetime = dbo.today()

	-- #ids
		if @folder_id = -1 set @folder_id = dbo.objs_buffer_id(@mol_id)
		create table #ids(id int primary key)
		exec objs_folders_ids @folder_id = @folder_id, @obj_type = @obj_type, @temp_table = '#ids'

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
		
		declare @buffer_id int = dbo.objs_buffer_id(@mol_id)

	-- prepare sql
		declare @sql nvarchar(max), @fields nvarchar(max)

		declare @where nvarchar(max) = concat(
			' where (mol_id = @mol_id) '
            , case when @acc_register_id is not null then concat(' and (x.acc_register_id =', @acc_register_id, ')') end
			, case when @stock_id is not null then 
                ' and x.product_id in (
                    select distinct product_id
                    from sdocs_r_stocks_turns
                    where mol_id = @mol_id and stock_id = @stock_id
                )'
              end
			, case when @search is not null then ' and (x.product_name like @search)' end			
			)
				
		declare @fields_base nvarchar(max) = N'		
			@mol_id int,
            @search nvarchar(max),
            @stock_id int
		'

		declare @join nvarchar(max) = N''
			+ case when @folder_id is not null or exists(select 1 from #ids) then ' join #ids i on i.id = x.item_id ' else '' end
			+ case when exists(select 1 from #search_ids) then ' join #search_ids i2 on i2.id = x.item_id' else '' end
			
		DECLARE @hint VARCHAR(50) = ''

		if @buffer_operation is null
		begin
			-- @rowscount
			set @sql = N'select @rowscount = count(*) from [base_view] x with(nolock) ' + @join + @where
			set @fields = @fields_base + ', @rowscount int out'

			set @sql = replace(replace(replace(@sql, '[pkey]', @pkey), '[base_view]', @base_view), '[obj_type]', @obj_type)
			set @sql = @sql + @hint

			exec sp_executesql @sql, @fields,
				@mol_id, @search, @stock_id,
				@rowscount out
		
			-- @order_by
			declare @order_by nvarchar(150) = N' order by x.product_name'
			if @sort_expression is not null set @order_by = N' order by ' + @sort_expression

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
			if @trace = 1 print 'SELECT: ' + @sql

			exec sp_executesql @sql, @fields,
				@mol_id, @search, @stock_id,
				@offset, @fetchrows

		end

		else begin
			set @rowscount = -1 -- dummy
			set @fields = @fields_base

            exec objs_buffer_viewhelper
                @buffer_operation = @buffer_operation, @obj_type = @obj_type, @base_view = @base_view, @pkey = @pkey, @join = @join, @where = @where,
                @fields = @fields out, @sql = @sql out			

            if @trace = 1 print 'build buffer: ' + @sql

            exec sp_executesql @sql, @fields,
                @mol_id, @today,
                @search, @stock_id,
                @buffer_id

		end -- buffer_operation

	exec drop_temp_table '#ids,#search_ids'
end
go
