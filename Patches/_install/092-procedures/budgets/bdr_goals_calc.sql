if object_id('bdr_goals_calc') is not null
	drop proc bdr_goals_calc
go
create proc [bdr_goals_calc]
as
begin

	declare @goals table (goal_id int primary key, parent_id int, short_name varchar(max))	

	insert into @goals(parent_id, goal_id, short_name)
	select parent_id, goal_id, short_name from bdr_goals

	;with s as (
		select parent_id, goal_id, short_name as path from @goals where parent_id is null
		union all
		select t.parent_id, t.goal_id, s.path  + '/' + t.short_name
		from @goals t
			inner join s on s.goal_id = t.parent_id
		)
		update t
		set path = s.path
		from bdr_goals t
			inner join s on s.goal_id = t.goal_id

	-- reorder number
	exec bdr_goals_reorder
end


GO
