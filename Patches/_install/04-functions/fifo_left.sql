if object_id('fifo_left') is not null  drop function fifo_left
go
CREATE FUNCTION [fifo_left](@fifoId [uniqueidentifier])
RETURNS  TABLE (
	[row_id] [int] NULL,
	[value] [float] NULL
) WITH EXECUTE AS CALLER
AS 
EXTERNAL NAME [Application.CLR].[UserDefinedFunctions].[FifoLeft]
GO
