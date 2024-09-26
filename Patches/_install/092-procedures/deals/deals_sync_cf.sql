IF OBJECT_ID('DEALS_SYNC_CF_HIST') IS NULL
	CREATE TABLE DEALS_SYNC_CF_HIST(
		ID INT IDENTITY PRIMARY KEY,
		REPLICATE_DATE DATETIME,
		NOTE VARCHAR(MAX),
		ADD_DATE DATETIME DEFAULT GETDATE()
	)
GO

if object_id('deals_sync_cf') is not null drop procedure deals_sync_cf
go
create proc deals_sync_cf
as
begin
	
	set nocount on;

	declare @replicate_date datetime = isnull((select max(replicate_date) from deals_sync_cf_hist), getdate())
	declare @ids as app_pkids

-- get changes
	insert into @ids(id)
	select distinct d.deal_id
	from deals d
		join cf.dbo.doc_bomagreex x on x.docno = d.number
	where x.UpdatedOn > @replicate_date

-- update timestamps
	update x
	set update_date = getdate(),
		update_mol_id = -25
	from deals x
		join @ids i on i.id = x.deal_id

-- save @replicate_date
	set @replicate_date = (select max(UpdatedOn) from cf.dbo.doc_bomagreex where UpdatedOn >= @replicate_date)
	insert into deals_sync_cf_hist(replicate_date) values(@replicate_date)
end
go
