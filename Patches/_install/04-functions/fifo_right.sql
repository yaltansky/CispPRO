if object_id('fifo_right') is not null  drop function fifo_right
go
CREATE FUNCTION [fifo_right](@fifoId [uniqueidentifier])
RETURNS  TABLE (
	[row_id] [int] NULL,
	[value] [float] NULL
) WITH EXECUTE AS CALLER
AS 
EXTERNAL NAME [Application.CLR].[UserDefinedFunctions].[FifoRight]
GO
