if object_id('dateadd_wh') is not null  drop function dateadd_wh
go
CREATE function [dateadd_wh](@count float, @date datetime)
--функция прибавляет количество часов (рабочего времени) @count к дате @date
returns datetime
as
begin
  
declare 
    @work_length int,
    @full_days_count int, 
    @frac_day_count float,
    @next_work_date datetime,
    @day_date smalldatetime,
    @start_work_time float,
    @end_work_time float
  
-- исходим из предположения, что рабочий день начинается в 9-30, заканчивается в 18-30 
-- длится 9 часов
  set @work_length = 9
  select 
    @start_work_time = 0.3958333333, -- 9:30
    @end_work_time = 0.7708333333 -- 18:30

  set @full_days_count = cast(@count as int) / @work_length

  if cast(@date as float) - floor(cast(@date as float)) >= @end_work_time
  begin
    --если время у @date позже 18-30, то ставим время равным 9-30 следующего рабочего дня
    select @next_work_date = min(day_date) 
    from calendar
    where type = 0 and day_date > dbo.getday(@date)

    set @date = dateadd(minute, 30, dateadd(hour, 9, dbo.getday(@next_work_date)))
  end

  if cast(@date as float) - floor(cast(@date as float)) < @start_work_time
  begin
    --если время у @date раньше 9-30, то принимаем время равным 9-30 того же 
    --(или ближайшего, если день не рабочий) рабочего дня
    select @next_work_date = min(day_date) 
    from calendar
    where type = 0 and day_date >= dbo.getday(@date)

    set @date = dateadd(minute, 30, dateadd(hour, 9, dbo.getday(@next_work_date)))
  end

-- work_days
	declare work_days cursor local forward_only for 
		select day_date
		from dbo.calendar
		where type = 0 and day_date >= dbo.getday(@date)
		order by day_date

	open work_days

		fetch next from work_days into @day_date

		while @@fetch_status = 0 and @full_days_count > 0
		begin
			set @full_days_count = @full_days_count - 1
			fetch next from work_days into @day_date
		end

	close work_days
	deallocate work_days
    
	set @frac_day_count = @count - cast(@count as int) / @work_length * @work_length
	set @date = @day_date - floor(cast(@date as float)) + cast(@date as float)
	-- теперь к @date нужно прибавить оставшуюся дробную часть рабочего дня @frac_day_count
 
	select @next_work_date = min(day_date) 
	from calendar
	where type = 0 and day_date > dbo.getday(@date) 

	set @date = @date + @frac_day_count / 24

	if cast(@date as float) - floor(cast(@date as float)) >= @end_work_time
		set @date = 
				@next_work_date
				+ @start_work_time
				+ ((cast(@date as float) - floor(cast(@date as float))) - @end_work_time)

	if cast(@date as float) - floor(cast(@date as float)) < @start_work_time
		set @date = 
				@next_work_date 
				+ 1
				+ ((cast(@date as float) - floor(cast(@date as float))) - @end_work_time + @start_work_time)

	return @date
end


GO
