if object_id('projects_calc') is not null drop proc projects_calc
go
create proc projects_calc
	@mol_id int,
	@projects_ids app_pkids readonly
as
begin

	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	declare @tid int; exec tracer_init 'projects_calc', @trace_id = @tid out
	declare @ids varchar(max) = (
		select cast(id as varchar) + ',' as [text()]
		from @projects_ids
		for xml path('')
		)
	exec tracer_log @tid, @ids

	-- calc projects by one
	declare c_todo cursor local read_only forward_only for 
		select project_id from projects
		where project_id in (select id from @projects_ids)

	declare @project_id int

	open c_todo
	fetch next from c_todo into @project_id

	while (@@fetch_status <> -1)
	begin
		if (@@fetch_status <> -2)
		begin
			print char(10) + 'project_tasks_calc @project_id = ' + cast(@project_id as varchar) + ' ...'
			begin try
				exec project_tasks_calc @mol_id = @mol_id, @project_id = @project_id, @gantt_only = 0
			end try
			begin catch
				declare @err nvarchar(max) = concat(
					'There was an error with recalc project ', @project_id, ':',
					error_message()
					)
				exec tracer_log @tid, @err
				raiserror(@err, 16, 1)
			end catch
		end

		fetch next from c_todo into @project_id
	end

	close c_todo; deallocate c_todo

	exec tracer_close @tid
end
go
