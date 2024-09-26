if object_id('date2period') is not null  drop function date2period
go
CREATE FUNCTION [date2period] (@date datetime)
RETURNS int
AS
BEGIN

	declare @res int
	select @res = period_id from periods where @date between date_start and dateadd(ms, 1, dateadd(day, 1, date_end))
	RETURN @res
end


GO
