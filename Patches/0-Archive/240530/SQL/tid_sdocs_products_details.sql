if object_id('tid_sdocs_products_details') is not null drop trigger tid_sdocs_products_details
go
create trigger tid_sdocs_products_details on SDOCS_PRODUCTS_DETAILS
for insert, delete as
begin
	set nocount on;

    declare @rows app_pkids
        insert into @rows
        select distinct detail_id
        from (
            select detail_id from inserted
            union all select detail_id from deleted
            ) x

    update x set
        has_details = case when exists(select 1 from sdocs_products_details where detail_id = x.detail_id) then 1 end
	from sdocs_products x
        join @rows r on r.id = x.detail_id
end
GO
