if object_id('objs_folder_rmrows') is not null drop proc objs_folder_rmrows
go
create proc objs_folder_rmrows
	@mol_id int,
	@folder_id int,
	@obj_type varchar(20),
	@obj_ids varchar(max)
as
begin
	
	declare @ids app_pkids

	insert into @ids select distinct item
	from dbo.str2rows(@obj_ids, ',')
	where try_cast(item as int) is not null

	delete x from objs_folders_details x
		join @ids i on i.id = x.obj_id
	where folder_id = @folder_id and obj_type = @obj_type
	
end
GO
