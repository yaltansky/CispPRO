if object_id('tasks_view_daily') is not null drop proc tasks_view_daily
go
create proc [tasks_view_daily]
	@mol_id int,
	@role_mol_id int = null,
	@d_from datetime = null,
	@d_to datetime = null,
	@search varchar(50) = null
as
begin

	set nocount on;

	if @d_from is null set @d_from = dbo.today() - 1
	if @d_to is null set @d_to = dbo.today() - 1

	select * from (
		select 
			P.PROJECT_ID,
			ISNULL(P.NAME, 'ЗАДАЧИ ВНЕ ПРОЕКТОВ') AS PROJECT_NAME,
			PT.TASK_ID AS PROJECT_TASK_ID,
			PT.TASK_NUMBER AS PROJECT_TASK_NUMBER,
			PT.NAME AS PROJECT_TASK_NAME,
			T.TASK_ID,
			H.D_ADD,
			H.MOL_ID,
			M.NAME AS ADDR_FROM,
			H.TO_MOLS AS ADDRS_TO,
			H.DESCRIPTION AS NOTE
		from tasks_hists h
			inner join mols m on m.mol_id = h.mol_id
			inner join tasks t on t.task_id = h.task_id
			left join projects_tasks pt on pt.task_id = t.project_task_id	
				left join projects p on p.project_id = pt.project_id
		where h.d_add between @d_from and @d_to
			and (@role_mol_id is null or (
					h.mol_id = @role_mol_id 
					or charindex(cast(@role_mol_id as varchar) + ',', h.to_mols_ids + ',') > 0
				))
			and (@search is null or charindex(@search, h.description) > 0 )
		) u
	order by 
		case when u.project_id is not null then 1 else 2 end,
		u.project_name, u.project_task_number, u.task_id, u.d_add
end
GO
