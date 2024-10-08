if object_id('mfr_drafts_calc_prices') is not null drop proc mfr_drafts_calc_prices
go
-- exec mfr_drafts_calc_prices @mol_id = 1000
create proc [mfr_drafts_calc_prices]
	@mol_id int,
	@trace bit = 0
as
begin

	set nocount on;

	begin
		declare @subject_id int = (select top 1 subject_id from mfr_plans where status_id = 1)
		if dbo.isinrole_byobjs(@mol_id,  'Mfr.Admin', 'SBJ', @subject_id) = 0
		begin
			raiserror('У Вас нет доступа к пересчёту состава изделия всех заказов плана в данном субъекте учёта.', 16, 1)
			return
		end
	end -- access

	begin

		declare @proc_name varchar(50) = object_name(@@procid)
		declare @tid int; exec tracer_init @proc_name, @trace_id = @tid out, @echo = @trace

		declare @tid_msg varchar(max) = concat(@proc_name, '.params:', 
			' @mol_id=', @mol_id
			)
		exec tracer_log @tid, @tid_msg

	end -- params

	-- prices
		declare @prices table(product_id int primary key, unit_name varchar(20), price float)
		insert into @prices(product_id, unit_name, price)
		select
			d.product_id,
			isnull(u2.name, u1.name),
			d.price / coalesce(uk.koef, dbo.product_ukoef(u2.name,u1.name), 1)
		from sdocs_products d
			join products p on p.product_id = d.product_id
				left join products_units u2 on u2.unit_id = p.unit_id
			join products_units u1 on u1.unit_id = d.unit_id
			left join products_ukoefs uk on uk.product_id = d.product_id and uk.unit_from = u2.name and uk.unit_to = u1.name
			join sdocs h on d.doc_id = h.doc_id
			join (
				select d.product_id, detail_id = max(d.detail_id)
				from sdocs_products d
					join sdocs h on d.doc_id = h.doc_id
				where h.type_id = 9 and
					isnull(d.price,0) > 0.001
				group by d.product_id
			) s on s.detail_id = d.detail_id
		where h.type_id = 9
			and d.product_id is not null
			and isnull(d.price,0) > 0.001

	-- update
		update x set unit_name = p.unit_name, item_price0 = p.price
		from sdocs_mfr_drafts x
			join @prices p on p.product_id = x.item_id
		where x.is_buy = 1

	final:
		exec tracer_close @tid
end
GO
