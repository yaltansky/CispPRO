if object_id('project_calc') is not null drop proc project_calc
go
CREATE PROCEDURE [project_calc]
	@tree_id [int],
	@project_id [int],
	@trace_allowed [bit]
WITH EXECUTE AS CALLER
AS
EXTERNAL NAME [Application.CLR].[StoredProcedures].[project_calc]
GO
