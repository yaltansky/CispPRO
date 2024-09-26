if object_id('replace_multiple') is not null drop function replace_multiple
go
create function replace_multiple(@expression nvarchar(max), @search nvarchar(1), @replace nvarchar(1))
returns nvarchar(max)
as begin
	return replace(replace(replace(@expression, @search, '<>'), '><', ''),'<>', @replace)
end
go
