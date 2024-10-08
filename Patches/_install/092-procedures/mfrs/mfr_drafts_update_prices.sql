if object_id('mfr_drafts_update_prices') is not null drop proc mfr_drafts_update_prices
go
create proc [mfr_drafts_update_prices]
as
begin
    
    EXEC SYS_SET_TRIGGERS 0

    declare @docs app_pkids
    insert into @docs select doc_id from mfr_sdocs where plan_status_id = 1

    -- update x set
    --     @price = pr.price / coalesce(uk.koef, dbo.product_ukoef(u.name, x.unit_name), 1),
    --     item_price0 = @price
    -- from sdocs_mfr_drafts x
    --     join mfr_items_prices pr on pr.product_id = x.item_id
    --         join products_units u on u.unit_id = pr.unit_id
    --         left join products_ukoefs uk on uk.product_id = pr.product_id and uk.unit_from = u.name and uk.unit_to = x.unit_name
    -- where is_buy = 1
    --     and isnull(x.item_price0,0) = 0
    
    update x set
        item_price0 = pr.price,
        item_value0 = pr.price * x.q_brutto_product
    from sdocs_mfr_contents x
        join (
            select 
                x.content_id,
                price = cast(pr.price / coalesce(uk.koef, dbo.product_ukoef(u.name, x.unit_name), 1) as decimal(18,2))
            from sdocs_mfr_contents x
                join @docs i on i.id = x.mfr_doc_id
                join mfr_items_prices pr on pr.product_id = x.item_id
                    join products_units u on u.unit_id = pr.unit_id
                    left join products_ukoefs uk on uk.product_id = pr.product_id and uk.unit_from = u.name and uk.unit_to = x.unit_name
            where is_buy = 1
        ) pr on pr.content_id = x.content_id
    where isnull(x.item_price0,0) != pr.price

    if @@rowcount > 0
    begin
        -- sum item_value0
        update x
        set item_value0 = xx.item_value0
        from sdocs_mfr_contents x
            join (
                select
                    r.content_id,
                    item_value0 = sum(isnull(r2.item_value0,0))
                from sdocs_mfr_contents r
                    join @docs i on i.id = r.mfr_doc_id
                    join sdocs_mfr_contents r2 on 
                            r2.mfr_doc_id = r.mfr_doc_id
                        and	r2.product_id = r.product_id
                        and r2.node.IsDescendantOf(r.node) = 1
                where r.has_childs = 1
                    and r2.has_childs = 0
                group by r.content_id
            ) xx on xx.content_id = x.content_id
            join @docs i on i.id = x.mfr_doc_id
        where isnull(x.item_value0,0) != xx.item_value0

        update x
        set item_value0_part = x.item_value0 / nullif(xx.item_value0,0)
        from sdocs_mfr_contents x
            join (
                select mfr_doc_id, product_id,
                    item_value0 = sum(isnull(item_value0,0))
                from sdocs_mfr_contents
                where has_childs = 0
                group by mfr_doc_id, product_id
            ) xx on xx.mfr_doc_id = x.mfr_doc_id and xx.product_id = x.product_id
            join @docs i on i.id = x.mfr_doc_id
        where isnull(x.item_value0_part,0) != x.item_value0 / nullif(xx.item_value0,0)
    end

    EXEC SYS_SET_TRIGGERS 1

end
GO
