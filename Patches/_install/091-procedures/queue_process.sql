if object_id('queue_process') is not null drop proc queue_process
go
create proc [queue_process]
	@queue_id uniqueidentifier
as
begin
	set nocount on;

	if exists(select 1 from queues where queue_id = @queue_id and process_start is null)
	begin
		update queues set process_start = getdate() where queue_id = @queue_id
		declare @sql_cmd nvarchar(max) = (select sql_cmd from queues where queue_id = @queue_id)

		begin try
			exec sp_executesql @sql_cmd
			update queues set process_end = getdate() where queue_id = @queue_id
		end try

		begin catch
			update queues set process_end = getdate(), errors = error_message() where queue_id = @queue_id
		end catch
	end
end
GO
