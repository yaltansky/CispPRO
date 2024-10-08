/****** Object:  Table [PA_EMPLOYEES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[PA_EMPLOYEES]') AND type in (N'U'))
BEGIN
CREATE TABLE [PA_EMPLOYEES](
	[EMPLOYEE_ID] [int] IDENTITY(1,1) NOT NULL,
	[SUBJECT_ID] [int] NOT NULL,
	[CATEGORY_ID] [int] NULL,
	[PERSON_ID] [int] NULL,
	[PERSON_ENTITY_ID] [int] NULL,
	[STAFF_POSITION_ID] [int] NULL,
	[NAME] [varchar](150) NULL,
	[PHONE] [varchar](50) NULL,
	[PHONE_LOCAL] [int] NULL,
	[PHONE_MOBILE] [varchar](20) NULL,
	[ROOM] [varchar](50) NULL,
	[STATUS_ID] [int] NULL,
	[DATE_HIRE] [datetime] NULL,
	[DATE_FIRE] [datetime] NULL,
	[SALARY] [float] NULL,
	[SALARY_CONST] [float] NULL,
	[SALARY_BONUS_VOLUME] [float] NULL,
	[SALARY_BONUS_SKILL] [float] NULL,
	[SALARY_BONUS_REGION] [float] NULL,
	[MOTIVATION_ID] [int] NULL,
	[FUND_ID] [int] NULL,
	[NOTE] [varchar](max) NULL,
	[HEAD_PERSON_ID] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[EMPLOYEE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_PA_EMPLOYEES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[PA_EMPLOYEES]') AND name = N'IX_PA_EMPLOYEES')
CREATE UNIQUE NONCLUSTERED INDEX [IX_PA_EMPLOYEES] ON [PA_EMPLOYEES]
(
	[PERSON_ENTITY_ID] ASC,
	[STAFF_POSITION_ID] ASC,
	[DATE_HIRE] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

/****** Object:  Trigger [tiud_pa_employees]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiud_pa_employees]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tiud_pa_employees] on [PA_EMPLOYEES]
for insert, update, delete
as
begin

	set nocount on;

	if update(staff_position_id)
	begin
		declare @rows table(staff_position_id int)

		insert into @rows(staff_position_id)
		select distinct staff_position_id
		from (
			select staff_position_id from inserted union all select staff_position_id from deleted
			) u
		where u.staff_position_id is not null

		update staff
		set fact_employees = (select count(*) from pa_employees where staff_position_id = staff.staff_position_id and date_fire is null)
		from pa_staff_positions staff
		where staff.staff_position_id in (select staff_position_id from @rows)

		-- MOLS.CHIEF_ID
		update mols
		set chief_id = pe2.person_id
		from mols
			inner join inserted i on i.person_id = mols.mol_id
				inner join pa_staff_positions sf on sf.staff_position_id = i.staff_position_id
					inner join pa_staff_positions sf2 on sf2.staff_position_id = sf.head_position_id
						inner join pa_employees pe2 on pe2.staff_position_id = sf2.staff_position_id and pe2.date_fire is null

		-- children: MOLS.CHIEF_ID
		update mols
		set chief_id = i.person_id
		from inserted i
			inner join pa_staff_positions sf on sf.head_position_id = i.staff_position_id
				inner join pa_employees pe on pe.staff_position_id = sf.staff_position_id and pe.date_fire is null
					inner join mols on mols.mol_id = pe.person_id
	end
	
	if update(person_entity_id) or update(staff_position_id)
	begin
		declare @name varchar(250)
		update e
		set @name = p.name + '', '' + lower(po.name),
			name = case when len(@name) > 47 then substring(@name, 1, 47) + ''...'' else @name end,
			person_id = p.person_id
		from pa_employees e
			inner join inserted i on i.employee_id = e.employee_id
			inner join pa_persons_entities pe on pe.person_entity_id = e.person_entity_id
				inner join pa_persons p on p.person_id = pe.person_id
			inner join pa_staff_positions sf on sf.staff_position_id = e.staff_position_id
				inner join pa_posts po on po.post_id = sf.post_id
	end

	-- Change PA_PERSONS_ENTITIES.STATUS_ID
	if update(person_entity_id) or update(date_fire)
	begin
		declare @pe_rows table(person_entity_id int)

		insert into @pe_rows(person_entity_id)
		select distinct person_entity_id
		from (
			select person_entity_id from inserted union all select person_entity_id from deleted
			) u

		update x
		set status_id = 
				case
					when exists(select 1 from pa_employees where person_entity_id = x.person_entity_id and date_fire is null) then 1
					else 2
				end
		from pa_persons_entities x
		where x.person_entity_id in (select person_entity_id from @pe_rows)
	end

	-- Cache some columns in mols
	update mols
	set branch_id = sf.branch_id,
		dept_id = sf.dept_id,
		post_id = sf.post_id
	from mols m
		inner join inserted i on i.person_id = m.mol_id
			inner join pa_staff_positions sf on sf.staff_position_id = i.staff_position_id
	where i.date_fire is null
		and sf.salary_rate = 1

end' 
GO
