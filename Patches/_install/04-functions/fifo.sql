if object_id('fifo') is not null  drop function fifo
go
CREATE FUNCTION [fifo](@fifoId [uniqueidentifier], @RightRowId [int], @RightValue [float], @LeftRowId [int], @LeftValue [float])
RETURNS  TABLE (
	[value] [float] NULL
) WITH EXECUTE AS CALLER
AS 
EXTERNAL NAME [Application.CLR].[UserDefinedFunctions].[FifoCalc]
GO
