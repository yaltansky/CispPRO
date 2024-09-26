﻿if object_id('objs_folder_addrows') is not null drop proc objs_folder_addrows
go
create proc objs_folder_addrows
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

	insert into objs_folders_details(folder_id, obj_type, obj_id, add_mol_id)
	select distinct @folder_id, @obj_type, id, @mol_id
	from @ids i
	where not exists(
		select 1 from objs_folders_details 
		where folder_id = @folder_id and obj_type = @obj_type
		and obj_id = i.id
		)

end
GO
