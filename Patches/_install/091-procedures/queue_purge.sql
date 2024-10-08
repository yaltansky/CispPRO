if object_id('queue_purge') is not null drop proc queue_purge
go
create proc [queue_purge]
as
begin

	set nocount on;

	if db_name() = 'CISP'
		delete from queues where group_name in ('jobs-queue-agent', 'jobs-details-agent')
			and process_end is not null

	-- удалить устаревшие ошибки
	delete from queues where errors is not null
		and datediff(minute, process_end, getdate()) > 30

	update queues set process_end = getdate(), cancel_date = getdate(),
		errors = 
			case
				when process_start is null then 'hanged queue item'
				else 'timeout expired, canceled automatically'
			end
	where group_name not like 'broker-agent%'
		and (
			-- timeout expired
			(process_start is not null and process_end is null and datediff(minute, process_start, getdate()) > 10)
			-- hanged queue items
			or (process_start is null and datediff(minute, add_date, getdate()) > 30)
		)
		
end
GO
