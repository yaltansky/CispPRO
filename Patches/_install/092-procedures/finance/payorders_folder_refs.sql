if object_id('payorders_folder_refs') is not null drop proc payorders_folder_refs
go
create proc [payorders_folder_refs]
	@mol_id int,
	@folder_id int
as
begin

	set nocount on;

	declare @ids as app_pkids; insert into @ids exec objs_folders_ids @folder_id = @folder_id, @obj_type = 'po'
	declare @buffer_id int = dbo.objs_buffer_id(@mol_id)

	delete from objs_folders_details where folder_id = @buffer_id
	
	insert into objs_folders_details(folder_id, obj_type, obj_id, add_mol_id)
	select @buffer_id, 'fd', x.findoc_id, @mol_id
	from (
		select distinct pp.findoc_id
		from payorders_pays pp
			join @ids i on i.id = pp.payorder_id
		) x

	insert into objs_folders_details(folder_id, obj_type, obj_id, add_mol_id)
	select distinct @buffer_id, 'bdg', x.budget_id, @mol_id
	from payorders_details x
		join @ids i on i.id = x.payorder_id		

	insert into objs_folders_details(folder_id, obj_type, obj_id, add_mol_id)
	select distinct @buffer_id, 'dl', d.deal_id, @mol_id
	from payorders_details x
		join @ids i on i.id = x.payorder_id
		join deals d on d.budget_id = x.budget_id
		
	insert into objs_folders_details(folder_id, obj_type, obj_id, add_mol_id)
	select distinct @buffer_id, 'prj', p.project_id, @mol_id
	from payorders_details x
		join @ids i on i.id = x.payorder_id
		join budgets b on b.budget_id = x.budget_id
			join projects p on p.project_id = b.project_id and p.type_id not in (3)

end
GO
