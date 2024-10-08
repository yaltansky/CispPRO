if object_id('objs_folder_oninsert') is not null drop proc objs_folder_oninsert
go
create proc [objs_folder_oninsert]
	@folder_id int
as begin

	update objs_folders set inherited_access = 1
	where folder_id = @folder_id				

	declare @inherited_access bit = (select inherited_access from objs_folders where folder_id = @folder_id)
	if @inherited_access = 1 exec objs_folders_calc_access @folder_id = @folder_id
end
GO
