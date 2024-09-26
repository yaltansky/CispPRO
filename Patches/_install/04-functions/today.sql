if object_id('today') is not null drop function today
GO
create function [today]() returns datetime 
as
begin 
	return dbo.getday(getdate()) 
end
GO
