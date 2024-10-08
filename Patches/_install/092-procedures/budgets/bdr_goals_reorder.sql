if object_id('bdr_goals_reorder') is not null drop proc bdr_goals_reorder
go
create proc bdr_goals_reorder
as
begin

	set nocount on;

	-- delete unused
	delete from bdr_goals where is_deleted = 1

-- Calc new order
	create table #root(goal_id int, sort_id float)
	insert into #root select goal_id, sort_id from bdr_goals where parent_id is null

	create table #plan (
		parent_id int
		, goal_id int primary key
		, sort_id float
	)	

	insert into #plan(goal_id, parent_id, sort_id)
		select goal_id, parent_id, sort_id from bdr_goals

	create table #sorted(row_id int identity, goal_id int primary key)

-- цикл по связям
	declare c_root cursor local read_only for 
		select goal_id from #root order by sort_id, goal_id

	declare @parent_id int
	
	open c_root
	fetch next from c_root into @parent_id

	while (@@fetch_status <> -1)
	begin
		if (@@fetch_status <> -2)
		begin
			
			insert into #sorted(goal_id) values(@parent_id)
			exec bdr_goals_reorder;2 @parent_id = @parent_id
		end

		fetch next from c_root into @parent_id
	end

	close c_root
	deallocate c_root

-- Set new order
	update pp
	set sort_id = tn.new_number
	from bdr_goals pp
		inner join (
			select goal_id, row_number() over (order by row_id) as 'new_number'
			from #sorted
		) tn on tn.goal_id = pp.goal_id

end
GO

create proc [bdr_goals_reorder];2
	@parent_id int
as
begin

	declare c_childs cursor local read_only for 
		select goal_id from #plan where parent_id = @parent_id
		order by sort_id, goal_id

	declare @goal_id int
	
	open c_childs
	fetch next from c_childs into @goal_id

	while (@@fetch_status <> -1)
	begin
		if (@@fetch_status <> -2)
		begin
			
			insert into #sorted(goal_id) values(@goal_id)

			if exists(select 1 from #plan where parent_id = @goal_id)
				exec bdr_goals_reorder;2 @parent_id = @goal_id
		end

		fetch next from c_childs into @goal_id
	end

	close c_childs
	deallocate c_childs

end
GO
