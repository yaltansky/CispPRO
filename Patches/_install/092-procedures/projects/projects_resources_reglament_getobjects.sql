if object_id('projects_resources_reglament_getobjects') is not null drop proc projects_resources_reglament_getobjects
go

create proc projects_resources_reglament_getobjects
	@mol_id int,
	@tree_id int = null
as
begin

	set nocount on;

	declare @tree table(tree_id int primary key)
	declare @tree_all table(tree_id int)
	declare @projects table(project_id int)

-- @subjects
	if @tree_id is not null
	begin
		insert into @tree select @tree_id

		;with tree as (
			select parent_id, tree_id from projects_trees where parent_id = @tree_id
			union all
			select t.parent_id, t.tree_id
			from projects_trees t
				inner join tree on tree.tree_id = t.parent_id
			)
			insert into @tree select tree_id from tree
	end

-- @projects
	insert into @tree_all
		-- @tree_id + all childs
		select tree_id from projects_trees where @tree_id is not null and tree_id in (select tree_id from @tree)
		-- + by @tree_id
		UNION select tree_id from projects_trees where @tree_id is null
			and (@tree_id is null or tree_id = @tree_id)  -- если не указано, то все проекты с учётом @tree_id

	if @tree_id is null
		insert into @projects select p.project_id from @tree_all t
			inner join projects_trees pt on pt.tree_id = t.tree_id
				inner join projects p on p.project_id = pt.obj_id
		where dbo.isinrole(@mol_id, 'Projects.Admin') = 1
	else
		insert into @projects select p.project_id from @tree_all t
			inner join projects_trees pt on pt.tree_id = t.tree_id
				inner join projects p on p.project_id = pt.obj_id
		where (
				dbo.isinrole(@mol_id, 'Projects.Admin') = 1
				or @mol_id in (p.admin_id, p.chief_id, p.curator_id)
				or @mol_id in (select mol_id from projects_mols where project_id = p.project_id)
			)

-- @result
	declare @result as app_objects

	insert @result(obj_type, obj_id) select 'PRJ', project_id from @projects

	select * from @result
end
go

