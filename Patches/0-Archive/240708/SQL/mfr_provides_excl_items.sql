if object_id('mfr_provides_excl_items') is not null drop function mfr_provides_excl_items
go
create function mfr_provides_excl_items()
returns @items table (id int primary key)
as
begin

    insert into @items
    select item from (
        select distinct item = try_cast(item as int)
        from dbo.str2rows(dbo.app_registry_varchar('MfrProvidesExcludeMaterialTypes'), ',')
        ) x 
    where item is not null

    return

end
go
