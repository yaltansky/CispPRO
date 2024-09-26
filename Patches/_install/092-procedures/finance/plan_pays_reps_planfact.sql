if object_id('plan_pays_reps_planfact') is not null drop proc plan_pays_reps_planfact
go
-- exec plan_pays_reps_planfact 700, 202109, 202109
create proc plan_pays_reps_planfact
	@mol_id int,
	@period_from varchar(16),
	@period_to varchar(16),
	@recalc bit = 0,
    @trace bit = 0
as
begin

	set nocount on;

	if @period_from is null set @period_from = (select period_id from periods where type_id = 'month' and dbo.today() between date_start and date_end)
	if @period_to is null set @period_to = @period_from

-- access
	declare @managers as app_pkids
	
	declare @is_admin bit = dbo.isinrole(@mol_id, 'Admin,Finance.Plans.Admin,Finance.Plans.Reader')
	if @is_admin = 0
	begin
		insert into @managers
		select mol_id from mols where @mol_id in (mol_id, chief_id)
            or mol_id in (
                select distinct mol_id from deals_mols 
                where deal_id in (select deal_id from projects_mols where mol_id = @mol_id) 
                )
	end

	if @recalc = 1 and @is_admin = 1 exec plan_pays_calc @mol_id = @mol_id, @period_id = @period_to

	select 
		subject_name = s.name,
		period_name = x.period_id,
		week_name = isnull(datepart(iso_week, x.d_doc), '-'),
		direction_name = coalesce(dir.short_name, dir.name, '-'),
		mol_name = m.name,
		vendor_name = isnull(v.short_name, '-'),
		agent_name = coalesce(ag.name, x.agent_name, '-'),
		consumer_name = coalesce(ag2.name, '-'),
		pay_type = isnull(ppt.name,''),
		x.d_doc,
		deal_number = d.number,
		x.note,
		x.value_plan,
		x.value_fact
	from plan_pays_az x
		left join subjects s on s.subject_id = x.subject_id
		left join depts dir on dir.dept_id = x.direction_id
		left join mols m on m.mol_id = x.mol_id
		left join vendors v on v.subject_id = x.vendor_id
		left join agents ag on ag.agent_id = x.agent_id
		left join deals d on d.deal_id = x.deal_id
			left join agents ag2 on ag2.agent_id = d.consumer_id
		left join plan_pays_types ppt on ppt.pay_type_id = x.pay_type_id
	where x.period_id between @period_from and @period_to
		and (@is_admin = 1
			or x.mol_id in (select id from @managers)
			or x.mol_id is null
			)

end
go
