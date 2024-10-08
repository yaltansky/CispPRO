/****** Object:  Table [MFR_PLANS_JOBS_EXECUTORS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EXECUTORS]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_PLANS_JOBS_EXECUTORS](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DETAIL_ID] [int] NULL,
	[MOL_ID] [int] NULL,
	[NAME] [varchar](150) NULL,
	[PLAN_DURATION_WK] [float] NULL,
	[PLAN_DURATION_WK_ID] [int] NULL,
	[DURATION_WK] [float] NULL,
	[DURATION_WK_ID] [int] NULL,
	[NOTE] [varchar](max) NULL,
	[D_DOC] [datetime] NULL,
	[OVERLOADS_DURATION_WK] [float] NULL,
	[POST_ID] [int] NULL,
	[RATE_PRICE] [float] NULL,
	[PLAN_Q] [float] NULL,
	[FACT_Q] [float] NULL,
	[WK_SHIFT] [varchar](10) NULL,
 CONSTRAINT [PK_MFR_PLAN_JOBS_EXECUTORS] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_MFR_PLANS_JOBS_EXECUTORS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EXECUTORS]') AND name = N'IX_MFR_PLANS_JOBS_EXECUTORS')
CREATE NONCLUSTERED INDEX [IX_MFR_PLANS_JOBS_EXECUTORS] ON [MFR_PLANS_JOBS_EXECUTORS]
(
	[DETAIL_ID] ASC,
	[MOL_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_MFR_PLANS_JOBS_EXECUTORS_NAME]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EXECUTORS]') AND name = N'IX_MFR_PLANS_JOBS_EXECUTORS_NAME')
CREATE NONCLUSTERED INDEX [IX_MFR_PLANS_JOBS_EXECUTORS_NAME] ON [MFR_PLANS_JOBS_EXECUTORS]
(
	[NAME] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_MFR_PLANS_JOBS_EXECUTORS2]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EXECUTORS]') AND name = N'IX_MFR_PLANS_JOBS_EXECUTORS2')
CREATE NONCLUSTERED INDEX [IX_MFR_PLANS_JOBS_EXECUTORS2] ON [MFR_PLANS_JOBS_EXECUTORS]
(
	[DETAIL_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_MFR_PLANS_JOBS_EXECUTORS_DETAIL_ID]') AND parent_object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EXECUTORS]'))
ALTER TABLE [MFR_PLANS_JOBS_EXECUTORS]  WITH CHECK ADD  CONSTRAINT [FK_MFR_PLANS_JOBS_EXECUTORS_DETAIL_ID] FOREIGN KEY([DETAIL_ID])
REFERENCES [MFR_PLANS_JOBS_DETAILS] ([ID])
ON DELETE CASCADE
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_MFR_PLANS_JOBS_EXECUTORS_DETAIL_ID]') AND parent_object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EXECUTORS]'))
ALTER TABLE [MFR_PLANS_JOBS_EXECUTORS] CHECK CONSTRAINT [FK_MFR_PLANS_JOBS_EXECUTORS_DETAIL_ID]
GO
/****** Object:  Trigger [tid_mfr_plans_jobs_executors]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tid_mfr_plans_jobs_executors]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tid_mfr_plans_jobs_executors] on [MFR_PLANS_JOBS_EXECUTORS]
for insert, update, delete as
begin

	set nocount on;
	if dbo.sys_triggers_enabled() = 0 return -- disabled

	create table #jexec_ids(id int primary key)
		insert into #jexec_ids
		select distinct detail_id from (
			select detail_id from inserted 
			union select detail_id from deleted
			) u

	if update(mol_id)
		or update(plan_q)
		or update(plan_duration_wk)
		or update(fact_q)
		or update(duration_wk)
		or not exists(select 1 from inserted)
		or not exists(select 1 from deleted)
	begin	
		update x
		set duration_wk = xx.duration_wk,
			duration_wk_id = 2,
			fact_q = isnull(xx.fact_q, x.fact_q),
			count_executors = xx.count_executors,
			executors_names = 
                case 
                    when xx.count_executors = 1 then xx.executors_names 
                    when xx.count_executors > 1 then concat(''исп '', xx.count_executors)
                end
		from mfr_plans_jobs_details x
			join #jexec_ids i on i.id = x.id
			left join (
				select 
					e.detail_id,
					executors_names = max(mols.name),
					duration_wk = sum(e.duration_wk * dur.factor24 / dur_h.factor24),
					fact_q = nullif(sum(e.fact_q), 0),
					count_executors = count(distinct e.mol_id)
				from mfr_plans_jobs_executors e with(nolock)
					left join projects_durations dur on dur.duration_id = e.duration_wk_id
					join projects_durations dur_h on dur_h.duration_id = 2
					join mols on mols.mol_id = e.mol_id
				group by e.detail_id
			) xx on xx.detail_id = x.id

		update q set count_executors = jd.count_executors, executors_names = jd.executors_names
		from mfr_plans_jobs_queues q
			join #jexec_ids i on i.id = q.detail_id
			join mfr_plans_jobs_details jd with(nolock) on jd.id = q.detail_id

		declare @details app_pkids
			insert into @details select id from #jexec_ids
			drop table #jexec_ids
		exec mfr_plans_jdetail_calc_queue @details = @details
	end
	
end' 
GO
