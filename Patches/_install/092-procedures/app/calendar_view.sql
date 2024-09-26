if object_id('calendar_view') is not null drop proc calendar_view
go

create proc calendar_view
	@date_from datetime,
	@date_to datetime = null,
	@duration int = null -- working days after @date_from
as
begin
	
	set nocount on;

	-- verify calendar
	exec calendar_calc @date_from, @date_to

	declare @calendar table (row_id int, day_date datetime)
	declare @workday_id int = (select top 1 workday_id from calendar where day_date >= @date_from and type = 0 order by day_date)

	if @duration is not null
	begin
		insert into @calendar (row_id, day_date)
		select top(@duration) workday_id - workday_id, day_date
		from calendar
		where day_date >= @date_from
			and type = 0
	end

	else begin
		insert into @calendar (row_id, day_date)
		select workday_id - workday_id, day_date
		from calendar
		where day_date between @date_from and @date_to
			and type = 0
	end

	select * from @calendar
end
go