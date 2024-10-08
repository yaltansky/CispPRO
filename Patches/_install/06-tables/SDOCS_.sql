/****** Object:  Table [SDOCS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[SDOCS]') AND type in (N'U'))
BEGIN
CREATE TABLE [SDOCS](
	[DOC_ID] [int] IDENTITY(1,1) NOT NULL,
	[TYPE_ID] [int] NULL,
	[STATUS_ID] [int] NULL,
	[SUBJECT_ID] [int] NULL,
	[D_DOC] [datetime] NULL,
	[D_DELIVERY] [datetime] NULL,
	[D_ISSUE] [datetime] NULL,
	[NUMBER] [varchar](50) NULL,
	[DEAL_ID] [int] NULL,
	[DEAL_NUMBER] [varchar](50) NULL,
	[AGENT_ID] [int] NULL,
	[AGENT_DOGOVOR] [varchar](50) NULL,
	[STOCK_ID] [int] NULL,
	[MOL_ID] [int] NULL,
	[CCY_ID] [char](3) NULL DEFAULT ('RUR'),
	[CCY_RATE] [float] NULL DEFAULT ((1)),
	[VALUE_CCY] [decimal](18, 2) NULL,
	[VALUE_RUR] [decimal](18, 2) NULL,
	[NOTE] [varchar](max) NULL,
	[REFKEY] [varchar](100) NULL,
	[ADD_MOL_ID] [int] NOT NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[CONTENT] [varchar](max) NULL,
	[EXTERN_ID] [varchar](100) NULL,
	[D_DELIVERY_PLAN] [datetime] NULL,
	[PLAN_ID] [int] NULL,
	[D_ISSUE_PLAN] [datetime] NULL,
	[D_ISSUE_CALC] [datetime] NULL,
	[D_ISSUE_FORECAST] [datetime] NULL,
	[D_CALC_CONTENTS] [datetime] NULL,
	[D_CALC_LINKS] [datetime] NULL,
	[PROJECT_ID] [int] NULL,
	[D_CALC_JOBS] [datetime] NULL,
	[D_CALC_JOBS_BUYS] [datetime] NULL,
	[PROJECT_TASK_ID] [int] NULL,
	[REPLICATE_DATE] [datetime] NULL,
	[SOURCE_ID] [int] NULL,
	[TEMPLATE_ID] [int] NULL,
	[TEMPLATE_NAME] [varchar](50) NULL,
	[TEMPLATE_NOTE] [varchar](max) NULL,
	[DOGOVOR_NUMBER] [varchar](50) NULL,
	[DOGOVOR_DATE] [date] NULL,
	[SPEC_NUMBER] [varchar](50) NULL,
	[SPEC_DATE] [date] NULL,
	[BUDGET_ID] [int] NULL,
	[PAY_CONDITIONS] [varchar](100) NULL,
	[PARENT_ID] [int] NULL,
	[D_SYNC_CHILDS] [datetime] NULL,
	[SYNC_DIRTY] [bit] NULL,
	[PRIORITY_ID] [int] NOT NULL DEFAULT ((500)),
	[D_SHIP] [date] NULL,
	[PART_PARENT_ID] [int] NULL,
	[PART_HAS_CHILDS] [bit] NULL,
	[PLACE_ID] [int] NULL,
	[PLACE_TO_ID] [int] NULL,
	[EXECUTOR_ID] [int] NULL,
	[PRIORITY_FINAL] [int] NULL,
	[PRIORITY_SORT] [varchar](30) NULL,
	[MOL_TO_ID] [int] NULL,
	[D_DOC_EXT] [date] NULL,
	[INVOICE_ID] [int] NULL,
	[ACC_REGISTER_ID] [int] NULL,
	[TALK_ID] [int] NULL,
	[HAS_INVOICE] [int] NULL DEFAULT ((1)),
	[EXT_TYPE_ID] [int] NULL,
	[EXT_PROBABILITY_ID] [int] NULL,
	[EXT_STATUS_ID] [int] NULL,
	[CONSIGNEE_ID] [int] NULL,
 CONSTRAINT [PK_SDOCS] PRIMARY KEY CLUSTERED 
(
	[DOC_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO
/****** Object:  Index [IX_EXTERN]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS]') AND name = N'IX_EXTERN')
CREATE NONCLUSTERED INDEX [IX_EXTERN] ON [SDOCS]
(
	[EXTERN_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_SDOCS_NUMBER]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS]') AND name = N'IX_SDOCS_NUMBER')
CREATE NONCLUSTERED INDEX [IX_SDOCS_NUMBER] ON [SDOCS]
(
	[NUMBER] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_SDOCS_PRIORITY_SORT]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS]') AND name = N'IX_SDOCS_PRIORITY_SORT')
CREATE NONCLUSTERED INDEX [IX_SDOCS_PRIORITY_SORT] ON [SDOCS]
(
	[PRIORITY_SORT] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_SDOCS_REFKEY]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS]') AND name = N'IX_SDOCS_REFKEY')
CREATE NONCLUSTERED INDEX [IX_SDOCS_REFKEY] ON [SDOCS]
(
	[REFKEY] ASC,
	[TYPE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_STATUS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS]') AND name = N'IX_SDOCS_STATUS')
CREATE NONCLUSTERED INDEX [IX_SDOCS_STATUS] ON [SDOCS]
(
	[STATUS_ID] ASC
)
INCLUDE ( 	[DOC_ID]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  FullTextIndex     Script Date: 9/18/2024 3:24:46 PM ******/
IF not EXISTS (SELECT * FROM sys.fulltext_indexes fti WHERE fti.object_id = OBJECT_ID(N'[SDOCS]'))
CREATE FULLTEXT INDEX ON [SDOCS](
[CONTENT] LANGUAGE 'English')
KEY INDEX [PK_SDOCS]ON ([CATALOG], FILEGROUP [PRIMARY])
WITH (CHANGE_TRACKING = AUTO, STOPLIST = SYSTEM)


GO
/****** Object:  Trigger [tiu_sdocs_content]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiu_sdocs_content]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tiu_sdocs_content] on [SDOCS]
for insert, update as
begin
	
	if dbo.sys_triggers_enabled() = 0 return -- disabled

	set nocount on;

	if update(type_id)
		update sdocs
		set refkey = 
				case
					when type_id = 5 then concat(''/mfrs/docs/'', doc_id)
				end
		where doc_id in (select doc_id from inserted)

	update x
	set content = concat(
			x.number, ''#''
			, ag.name, ''#''
			, mols.name, ''#''
			, abs(x.value_ccy), ''#''
			, x.note
			)
	from sdocs x
		left join agents ag on ag.agent_id = x.agent_id
		left join mols on mols.mol_id = x.mol_id
	where doc_id in (
		select doc_id from inserted union select doc_id from deleted
		)
end
' 
GO
/****** Object:  Trigger [tiu_sdocs_mfr_number]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiu_sdocs_mfr_number]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tiu_sdocs_mfr_number] on [SDOCS]
for insert, update as
begin

	set nocount on;
	if dbo.sys_triggers_enabled() = 0 return -- disabled

	if update(number)
		and	exists(
				select 1
				from sdocs
				where type_id = 5
					and number is not null
					and number in (select distinct number from inserted where number is not null)
					and number not in (''120504-094РЛ'') -- исключение, поскольку с этим документом экспериментировал Васильев Дмитрий
				group by number
				having count(*) > 1
			)
	begin
		raiserror(''Номера производственных заказов должны быть уникальны.'', 16, 1)
		rollback
	end

end
' 
GO
/****** Object:  Trigger [tiu_sdocs_objs]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiu_sdocs_objs]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tiu_sdocs_objs] on [SDOCS]
for insert, update as
begin

	set nocount on;

	if update(plan_id)
		update x set plan_id = mfr.plan_id
		from sdocs_mfr_contents x
			join inserted mfr on mfr.doc_id = x.mfr_doc_id
		where x.plan_id != mfr.plan_id

	if update(priority_id) or update(d_ship) or update(status_id)
	begin
		declare @priority_id int
		update sdocs
		set @priority_id = case when status_id = 100 then -1 else priority_id end,
			priority_final = @priority_id,
			priority_sort = concat(
				case when @priority_id = -1 then ''*'' else right(concat(''00000'', @priority_id), 5) end,
				'':'',
				left(convert(varchar, d_ship, 20), 10)
			)
		where doc_id in (select doc_id from inserted)
	end

	if not exists(
		select 1 from objs o
			join inserted i on o.owner_type = ''mfr'' and i.doc_id = o.owner_id
		)
		insert into objs(owner_type, owner_id, owner_name)
		select ''mfr'', doc_id, concat(''Производственный заказ №'', number)
		from inserted
		where type_id = 5
	
	else 
		update o set owner_name = concat(''Производственный заказ №'', number)
		from objs o
			join inserted i on o.owner_type = ''mfr'' and i.doc_id = o.owner_id

	if dbo.sys_triggers_enabled() = 0 return -- disabled

	if update(project_task_id)
		update x
		set refkey = concat(''/mfrs/docs/'', i.doc_id)
		from projects_tasks x
			join inserted i on i.project_task_id = x.task_id

	if update(parent_id)
	begin
		-- Всё множество связанных документов с мастером и сам мастер-документ
		-- автоматически отключаются от репликации

		declare @parents as app_pkids
			insert into @parents 
			select doc_id from sdocs where doc_id in (
				select parent_id from inserted where parent_id is not null
				union select doc_id from deleted where parent_id is not null
				)

		update x set
			source_id = 
				case
					when exists(select 1 from sdocs where parent_id = x.doc_id)
						or parent_id is not null
							then 1 -- КИСП (т.е. отключаем от репликации)
					else
						2 -- Внешний источник
				end			
		from sdocs x
		where doc_id in (
			select id from @parents
			union select doc_id from sdocs where parent_id in (select id from @parents)
			)
	end

end' 
GO
