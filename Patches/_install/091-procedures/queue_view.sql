if object_id('queue_view') is not null drop proc queue_view
go
-- exec queue_view 'cisp_sez', @search = 'running'
create proc [queue_view]
	@dbname varchar(32),
	@thread_id varchar(32) = null,
	@search nvarchar(500) = null,
	-- sorting, paging
	@sort_expression varchar(50) = null,
	@offset int = 0,
	@fetchrows int = 30,
	--
	@rowscount int = null out
as
begin

    set nocount on;
	set transaction isolation level read uncommitted;

-- prepare sql
	declare @sql nvarchar(max), @fields nvarchar(max)

	declare @where nvarchar(max) = concat(
		' where (dbname = @dbname and thread_id = isnull(@thread_id, thread_id))'
		, case
			when @search is not null then ' and (
				charindex(@search, concat(group_name, name, mol_name, state, sql_cmd)) > 0
				)'
		  end
		)

	declare @fields_base nvarchar(max) = N'		
		@dbname varchar(32),
		@thread_id varchar(32),
		@search nvarchar(500)
	'

	declare @query nvarchar(max) = N'(select * from v_queues) x ' + @where

	-- @rowscount
	set @sql = N'select @rowscount = count(*) from ' + @query
	set @fields = @fields_base + ', @rowscount int out'

	exec sp_executesql @sql, @fields,
		@dbname, @thread_id, @search,
		@rowscount out

	-- @order_by
	declare @order_by nvarchar(50) = N' order by x.id desc'
	if @sort_expression is not null set @order_by = N' order by ' + @sort_expression

	-- @sql
	set @sql = 'select * from ' + @query + @order_by

	-- optimize on fetch
	if @rowscount > @fetchrows set @sql = @sql + ' offset @offset rows fetch next @fetchrows rows only'

	set @fields = @fields_base + ', @offset int, @fetchrows int'

	exec sp_executesql @sql, @fields,
		@dbname, @thread_id, @search,
		@offset, @fetchrows

end
GO
