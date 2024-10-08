/****** Object:  Table [SDOCS_MFR_OPERS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND type in (N'U'))
BEGIN
CREATE TABLE [SDOCS_MFR_OPERS](
	[OPER_ID] [int] IDENTITY(1,1) NOT NULL,
	[MFR_DOC_ID] [int] NULL,
	[PRODUCT_ID] [int] NULL,
	[CHILD_ID] [int] NULL,
	[CONTENT_ID] [int] NULL,
	[ROUTE_ID] [int] NULL,
	[STATUS_ID] [int] NULL,
	[PLACE_ID] [int] NULL,
	[TYPE_ID] [int] NULL,
	[NAME] [varchar](100) NULL,
	[NUMBER] [int] NULL,
	[DURATION] [float] NULL,
	[DURATION_ID] [int] NULL,
	[DURATION_WK] [float] NULL,
	[DURATION_WK_ID] [int] NULL,
	[D_FROM] [datetime] NULL,
	[D_TO] [datetime] NULL,
	[D_TO_FACT] [datetime] NULL,
	[PLAN_Q] [float] NULL,
	[FACT_Q] [float] NULL,
	[FACT_DEFECT_Q] [float] NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[EXTERN_ID] [varchar](32) NULL,
	[PREDECESSORS] [varchar](100) NULL,
	[IS_FIRST] [bit] NULL,
	[IS_LAST] [bit] NULL,
	[D_AFTER] [datetime] NULL,
	[D_BEFORE] [datetime] NULL,
	[DURATION_BUFFER] [int] NULL,
	[NEXT_ID] [int] NULL,
	[PREV_ID] [int] NULL,
	[PREDECESSORS_DEF] [varchar](100) NULL,
	[D_FROM_PREDICT] [datetime] NULL,
	[D_TO_PREDICT] [datetime] NULL,
	[DURATION_BUFFER_PREDICT] [int] NULL,
	[MILESTONE_ID] [int] NULL,
	[IS_VIRTUAL] [bit] NULL,
	[PROGRESS] [float] NULL,
	[RESOURCES_VALUE] [decimal](18, 2) NULL,
	[PROJECT_TASK_ID] [int] NULL,
	[WORK_TYPE_ID] [int] NULL,
	[D_FROM_PLAN] [datetime] NULL,
	[D_TO_PLAN] [datetime] NULL,
	[OPERKEY] [varchar](20) NULL,
	[MFR_NUMBER] [varchar](50) NULL,
	[MILESTONE_NAME] [varchar](50) NULL,
	[ITEM_NAME] [varchar](500) NULL,
	[RESOURCE_ID] [int] NULL,
	[D_FROM_PLOPER] [date] NULL,
	[D_TO_PLOPER] [date] NULL,
	[PROJECT_TASK_NAME] [varchar](50) NULL,
 CONSTRAINT [PK_SDOCS_MFR_OPERS] PRIMARY KEY CLUSTERED 
(
	[OPER_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO

/****** Object:  Index [IX_SDOCS_MFR_OPERS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS] ON [SDOCS_MFR_OPERS]
(
	[MFR_DOC_ID] ASC,
	[CHILD_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_MFR_OPERS_CONTENTS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS_CONTENTS')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS_CONTENTS] ON [SDOCS_MFR_OPERS]
(
	[CONTENT_ID] ASC,
	[NUMBER] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_SDOCS_MFR_OPERS_KEY]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS_KEY')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS_KEY] ON [SDOCS_MFR_OPERS]
(
	[MFR_DOC_ID] ASC,
	[CONTENT_ID] ASC,
	[OPERKEY] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_MFR_OPERS_MFR_DOC]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS_MFR_DOC')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS_MFR_DOC] ON [SDOCS_MFR_OPERS]
(
	[MFR_DOC_ID] ASC
)
INCLUDE ( 	[OPER_ID],
	[CONTENT_ID],
	[PROGRESS]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_MFR_OPERS_MILESTONES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS_MILESTONES')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS_MILESTONES] ON [SDOCS_MFR_OPERS]
(
	[MILESTONE_ID] ASC
)
INCLUDE ( 	[MFR_DOC_ID],
	[CONTENT_ID]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_MFR_OPERS_PLACE]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS_PLACE')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS_PLACE] ON [SDOCS_MFR_OPERS]
(
	[PLACE_ID] ASC
)
INCLUDE ( 	[CONTENT_ID]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_MFR_OPERS_PLACES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS_PLACES')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS_PLACES] ON [SDOCS_MFR_OPERS]
(
	[CONTENT_ID] ASC,
	[PLACE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_MFR_OPERS4]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS4')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS4] ON [SDOCS_MFR_OPERS]
(
	[PREV_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_MFR_OPERS5]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_OPERS]') AND name = N'IX_SDOCS_MFR_OPERS5')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_OPERS5] ON [SDOCS_MFR_OPERS]
(
	[PROJECT_TASK_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Trigger [ti_sdocs_mfr_opers]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[ti_sdocs_mfr_opers]'))
EXEC dbo.sp_executesql @statement = N'create trigger [ti_sdocs_mfr_opers] on [SDOCS_MFR_OPERS]
for insert as
begin

	set nocount on;

	update c set 
		work_type_1 = case when exists(select 1 from sdocs_mfr_opers where content_id = c.content_id and work_type_id = 1) then 1 end,
		work_type_2 = case when exists(select 1 from sdocs_mfr_opers where content_id = c.content_id and work_type_id = 2) then 1 end,
		work_type_3 = case when exists(select 1 from sdocs_mfr_opers where content_id = c.content_id and work_type_id = 3) then 1 end
	from sdocs_mfr_contents c
	where c.content_id in (select content_id from inserted)

end' 
GO
/****** Object:  Trigger [tid_sdocs_mfr_opers]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tid_sdocs_mfr_opers]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tid_sdocs_mfr_opers] on [SDOCS_MFR_OPERS]
for insert, delete as
begin

	set nocount on;

	update x
	set opers_count = isnull(op.opers_count, 0)
	from sdocs_mfr_contents x
		left join (
			select content_id, count(*) as opers_count
			from sdocs_mfr_opers
			group by content_id
		) op on op.content_id = x.content_id
	where x.content_id in (
		select content_id from inserted
		union select content_id from deleted
		)
end' 
GO
/****** Object:  Trigger [tiu_sdocs_mfr_opers]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiu_sdocs_mfr_opers]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tiu_sdocs_mfr_opers] on [SDOCS_MFR_OPERS]
for insert,update as
begin

	set nocount on;

	update x set
		mfr_number = left(sd.number, 50),
		milestone_name = left(ms.name, 50),
		item_name = c.name
	from sdocs_mfr_opers x
		join inserted i on i.oper_id = x.oper_id
		join sdocs sd on sd.doc_id = x.mfr_doc_id
		join sdocs_mfr_contents c on c.content_id = x.content_id
		left join mfr_milestones ms on ms.milestone_id = x.milestone_id

	update x
	set is_milestone = case when ms.content_id is not null then 1 end
	from sdocs_mfr_contents x
		left join (
			select distinct content_id
			from sdocs_mfr_opers
			where milestone_id is not null
		) ms on ms.content_id = x.content_id
	where x.content_id in (
		select distinct content_id from inserted
		)

end' 
GO
