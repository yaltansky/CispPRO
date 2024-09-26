if object_id('week_end') is not null drop function week_end
GO
create function [week_end](@date datetime) returns datetime
as
begin
	return dateadd(d, -(datepart(dw, @date) - 2), @date) + 6
end
GO
