/****** Object:  Table [SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]') AND type in (N'U'))
BEGIN
CREATE TABLE [SDOCS_MFR_DRAFTS_OPERS_EXECUTORS](
	[ID] [bigint] IDENTITY(1,1) NOT NULL,
	[DRAFT_ID] [int] NULL,
	[OPER_ID] [int] NULL,
	[MOL_ID] [int] NULL,
	[DURATION_WK] [float] NULL,
	[DURATION_WK_ID] [int] NULL,
	[NOTE] [varchar](max) NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[IS_DELETED] [bit] NULL,
	[RESOURCE_ID] [int] NULL,
	[POST_ID] [int] NULL,
	[RATE_PRICE] [float] NULL,
 CONSTRAINT [PK_SDOCS_MFR_DRAFTS_OPERS_EXECUTORS] PRIMARY KEY NONCLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]') AND name = N'IX_SDOCS_MFR_DRAFTS_OPERS_EXECUTORS')
CREATE CLUSTERED INDEX [IX_SDOCS_MFR_DRAFTS_OPERS_EXECUTORS] ON [SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]
(
	[DRAFT_ID] ASC,
	[OPER_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_MFR_DRAFTS_OPERS_EXECUTORS_OPER]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]') AND name = N'IX_SDOCS_MFR_DRAFTS_OPERS_EXECUTORS_OPER')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_DRAFTS_OPERS_EXECUTORS_OPER] ON [SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]
(
	[OPER_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Trigger [tg_sdocs_mfr_drafts_opers_executors]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tg_sdocs_mfr_drafts_opers_executors]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tg_sdocs_mfr_drafts_opers_executors] on [SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]
for insert, update, delete as
begin

	set nocount on;
	if dbo.sys_triggers_enabled() = 0 return -- disabled

	update x
	set duration_wk = xx.duration_wk,
		duration_wk_id = 2,
		count_executors = xx.c_rows
	from sdocs_mfr_drafts_opers x
		left join (
			select oper_id,
				sum(e.duration_wk * dur1.factor24 / dur2.factor24) as duration_wk,
				count(*) as c_rows
			from sdocs_mfr_drafts_opers_executors e
				join projects_durations dur1 on dur1.duration_id = e.duration_wk_id
				join projects_durations dur2 on dur2.duration_id = 2
			group by oper_id
		) xx on xx.oper_id = x.oper_id
	where x.oper_id in (
		select oper_id from inserted
		union select oper_id from deleted
		)
end' 
GO
/****** Object:  Trigger [tid_sdocs_mfr_drafts_opers_executors]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tid_sdocs_mfr_drafts_opers_executors]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tid_sdocs_mfr_drafts_opers_executors] on [SDOCS_MFR_DRAFTS_OPERS_EXECUTORS]
for insert, update, delete as
begin

	set nocount on;
	if dbo.sys_triggers_enabled() = 0 return -- disabled

	update x
	set duration_wk = xx.duration_wk,
		duration_wk_id = 2,
		count_executors = xx.c_rows
	from sdocs_mfr_drafts_opers x
		left join (
			select oper_id,
				sum(e.duration_wk * dur1.factor24 / dur2.factor24) as duration_wk,
				count(*) as c_rows
			from sdocs_mfr_drafts_opers_executors e
				join projects_durations dur1 on dur1.duration_id = e.duration_wk_id
				join projects_durations dur2 on dur2.duration_id = 2
			group by oper_id
		) xx on xx.oper_id = x.oper_id
	where x.oper_id in (
		select oper_id from inserted
		union select oper_id from deleted
		)
end' 
GO
