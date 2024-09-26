﻿if object_id('periods_fill') is not null drop proc periods_fill
go
create proc periods_fill(@date_from datetime, @date_to datetime)
as
begin

	declare @d datetime = @date_from

	while @d <= @date_to
	begin
	
		insert into periods(type_id, period_id, name, date_start, date_end)
		values(
			'MONTH',
			concat(year(@d), right('00' + cast(datepart(m, @d) as varchar), 2)),
			concat(year(@d), '-', right('00' + cast(datepart(m, @d) as varchar), 2)),
			@d,
			dateadd(m, 1, @d) -1
			)
		set @d = dateadd(m, 1, @d)

	end

end
go
