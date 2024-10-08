if object_id('month_name') is not null drop function month_name
go
create function [month_name](@date datetime)
returns varchar(10)
as
begin

	declare @month int; set @month = datepart(m, @date)
	declare @res varchar(12)
	
	set @res = 
		case
			when @month = 1  then 'январь'
			when @month = 2  then 'февраль'
			when @month = 3  then 'март'
			when @month = 4  then 'апрель'
			when @month = 5  then 'май'
			when @month = 6  then 'июнь'
			when @month = 7  then 'июль'
			when @month = 8  then 'август'
			when @month = 9  then 'сентябрь'
			when @month = 10 then 'октябрь'
			when @month = 11 then 'ноябрь'
			when @month = 12 then 'декабрь'
		end

	return @res
end
GO
