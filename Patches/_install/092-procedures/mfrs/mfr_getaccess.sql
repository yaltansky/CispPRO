if object_id('mfr_getaccess') is not null drop proc mfr_getaccess
go
create proc [mfr_getaccess]
	@mol_id int,
	@item varchar(64),
    @action varchar(64) = null,
    @subject_id int = null
as
begin

	declare @roles varchar(max) = isnull(
		dbo.app_registry_varchar(concat(
			@item,
			case when @action is not null then ':' end,
			@action)
		),
		'Admin,Mfr.Admin'
		)

    if @subject_id is null
        set @subject_id = (select top 1 subject_id from mfr_plans where status_id = 1)

	if dbo.isinrole_byobjs(@mol_id, @roles, 'SBJ', @subject_id) = 0
		raiserror('У Вас нет доступа к модерации объектов в данном контексте.', 16, 1)

end
GO
