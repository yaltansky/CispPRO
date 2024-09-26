if object_id('prettydecimal') is not null drop function prettydecimal
GO
create function [prettydecimal](@value decimal(18,2))
returns varchar(50)
as
begin
	declare @return varchar(50) = replace(convert(varchar, cast(sum(@value) as money), 1), ',', ' ')
	return @return
end
GO
