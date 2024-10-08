if object_id('findocs#_sync_push') is not null drop proc findocs#_sync_push
go
create proc [findocs#_sync_push]
	@ids app_pkids readonly
as
begin
	
	set nocount on;

	insert into findocs#todo select id from @ids

	declare @param varchar(10) = 'dummy'
	declare @h uniqueidentifier

	begin dialog @h
		from service Findocs#SyncService
		to service N'Findocs#SyncService'
        on contract Findocs#SyncContract
        with encryption = off;
	send on conversation @h
		message type Findocs#SyncRequestMessage(@param);
end
GO
