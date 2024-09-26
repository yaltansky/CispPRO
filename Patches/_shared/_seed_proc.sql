USE CISP_SHARED
go

if object_id('db_seed_build') is not null drop proc db_seed_build
go
create proc db_seed_build
as
begin
	set nocount on;

    declare @excludes table(name sysname primary key)
        insert into @excludes
        values 
            ('APP_DATABASES'),
            ('APP_DATABASES_SUBJECTS'),
            ('CALENDAR'),
            ('CCY_RATES'),
            ('CCY_RATES_CROSS'),
            ('QUEUES'),
            ('QUEUES_OBJS'),
            ('Users'),
            ('UsersLogs'),
            ('UsersRoles'),
            ('UsersRoles'),
            ('UsersSettings')

	declare @tables table(name sysname primary key, where_expression varchar(max))
	insert into @tables(name) select name from sys.tables


    update @tables set where_expression = 'ARTICLE_ID = 0' where name = 'BDR_ARTICLES'
    update @tables set where_expression = 'id in (-25,700,1000)' where name = 'Users'

    declare c_tables cursor local read_only for 
        select name, where_expression from @tables

        declare @name sysname, @where varchar(max)

        open c_tables
        fetch next from c_tables into @name, @where

        while (@@fetch_status <> -1)
        begin
            if (@@fetch_status <> -2)
            begin
                declare @seed varchar(max)
                exec db_seed_build;10 @name, @where, @seedscript = @seed out
                print @seed
            end

            fetch next from c_tables into @name, @where
        end
    close c_tables; deallocate c_tables	
end
go

create proc db_seed_build;10
	@table_name sysname,
	@where varchar(max) = null,
	@seedscript varchar(max) out
as
begin

	declare @has_identity bit = 
		case 
			when exists(select 1 from sys.columns where object_id = object_id(@table_name) and is_identity = 1) then 1
		end
	
	declare @last_column int = (select max(column_id) from sys.columns where object_id = object_id(@table_name))
	declare c_columns cursor local read_only for 
		select name, user_type_id, column_id from sys.columns where object_id = object_id(@table_name) order by column_id

	declare @columns varchar(max) = ''
	select @columns = @columns + name + ',' from sys.columns where object_id = object_id(@table_name) order by column_id
	set @columns = substring(@columns, 1, len(@columns) - 1)
	
	declare @name sysname, @type int, @column_id int
	declare @sql varchar(max) = '-- SEED: ' + @table_name + char(10)

	if @has_identity = 1
		set @sql = @sql + ';set identity_insert ' + @table_name + ' on;' + char(10)

	set @sql = @sql + 'insert into ' + @table_name + '(' + @columns + ') values'  + char(10)

	declare @sql_values nvarchar(max) = N'select @values = @values + ''('''

	open c_columns
	fetch next from c_columns into @name, @type, @column_id

	while (@@fetch_status <> -1)
	begin
		if (@@fetch_status <> -2)
		begin
			if @type in (167, 175, 231)
				set @sql_values = @sql_values + ' + isnull('''''''' + replace(' + @name + ', '''''''', '''''''''''') + '''''''', ''null'')'
			else if @type = 61
				set @sql_values = @sql_values + ' + isnull('''''''' + convert(varchar,' + @name + ', 20) + '''''''', ''null'')'
			else if @type = 128
				set @sql_values = @sql_values + ' + isnull('''''''' + cast(' + @name + ' as varchar) + '''''''', ''null'')'
			else 
				set @sql_values = @sql_values + ' + isnull(cast(' + @name + ' as varchar), ''null'')'

			if @column_id = @last_column
			begin
				set @sql_values = @sql_values 
					+ '+'')'''
					+ '+'','' + char(10)'
			end	
			else begin
				if @type in (61, 167, 175, 231)
					set @sql_values = @sql_values + ' + '',''' + char(10)
				else 
					set @sql_values = @sql_values + '+'',''' + char(10)
			end

		end

		fetch next from c_columns into @name, @type, @column_id
	end

	close c_columns
	deallocate c_columns
		
	set @sql_values = @sql_values + ' from ' + @table_name
	if @where is not null set @sql_values = @sql_values + ' where ' + @where

	declare @values varchar(max) = ''
	exec sp_executesql @sql_values, N'@values varchar(max) out', @values out

	set @values = substring(@values, 1, len(@values) - 2)
	
	set @sql = @sql + @values + char(10)
	
	if @has_identity = 1
		set @sql = @sql	+ ';set identity_insert ' + @table_name + ' off;' + char(10)

	set @seedscript = @sql
end
go

-- exec db_seed_build
