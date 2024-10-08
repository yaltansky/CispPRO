if object_id('deal_value') is not null drop procedure deal_value
go
create proc deal_value
    @deal_id int,
    @key varchar(50),
    @value varchar(max) out
as
begin
	declare @sql nvarchar(100) = N'select @value = %column from deals where deal_id = @deal_id'
	set @sql = replace(@sql, '%column', @key)
	exec sp_executesql @sql, N'@deal_id int, @value varchar(max) out', @deal_id, @value out
end
GO
