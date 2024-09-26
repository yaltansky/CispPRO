if object_id('projects_resources_calc') is not null drop proc projects_resources_calc
go
create proc [projects_resources_calc]
as
begin
	exec tree_calc_nodes 'projects_resources', 'resource_id', @sortable = 0
end
GO
