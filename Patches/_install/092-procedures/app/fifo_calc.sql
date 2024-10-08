if object_id('fifo_calc') is not null drop proc fifo_calc
go
create procedure fifo_calc
	@left fifo_request readonly,
	@right fifo_request readonly,
	@cross fifo_cross readonly 
as
begin

	set nocount on

	declare @l as fifo_request	insert into @l select * from @left
	declare @r as fifo_request	insert into @r select * from @right
	
	declare @result as fifo_response

	insert into @result (left_row_id, right_row_id) select left_row_id, right_row_id from @cross order by [order_id]

	insert into @result (left_row_id, right_row_id)
	select l.row_id, r.row_id
	from @l l
		inner join @r r on l.group_id = r.group_id
	order by l.row_id, r.row_id

	declare @i int set @i = 0

	-- c_cross
	declare c_cross cursor dynamic read_only for 
		select left_row_id, l.value as left_value, right_row_id, r.value as right_value
		from @result c
			inner join @l l on l.row_id = c.left_row_id
			inner join @r r on r.row_id = c.right_row_id
		where l.value > 0.0 and r.value > 0.0

	declare @left_row_id int, @left_value float
		, @right_row_id int, @right_value float

	open c_cross
	fetch next from c_cross into @left_row_id, @left_value, @right_row_id, @right_value

	while (@@fetch_status <> -1)
	begin
		set @i = @i + 1

		if (@@fetch_status <> -2)
		begin
			if @right_value >= @left_value begin
				update @result set value = @left_value where left_row_id = @left_row_id and right_row_id = @right_row_id
				update @r set value = value - @left_value where row_id = @right_row_id
				update @l set value = 0.0 where row_id = @left_row_id
			end
			else begin
				update @result set value = @right_value where left_row_id = @left_row_id and right_row_id = @right_row_id
				update @r set value = 0.0 where row_id = @right_row_id
				update @l set value = value - @right_value where row_id = @left_row_id
			end
		end

		fetch next from c_cross into @left_row_id, @left_value, @right_row_id, @right_value
	end

	close c_cross
	deallocate c_cross

	select * from @result where value is not null
end
GO
