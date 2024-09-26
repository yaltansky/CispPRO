if object_id('objs_search') is not null drop proc objs_search
go
create proc objs_search
	@search nvarchar(250)
as
begin
	
	declare @isfulltextinstalled bit
	select @isfulltextinstalled = fulltextserviceproperty('isfulltextinstalled') 

	if @isfulltextinstalled = 1
	begin
		set @search = '"' + replace(@search, '"', '*') + '"'		
		select obj_uid from objs where contains(content, @search)
	end

	else begin
		set @search = '%' + @search + '%'
		select obj_uid from objs where content like @search
	end

end
GO