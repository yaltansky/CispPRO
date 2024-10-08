/****** Object:  Table [MFR_PLANS_JOBS_EQUIPMENTS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EQUIPMENTS]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_PLANS_JOBS_EQUIPMENTS](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DETAIL_ID] [int] NULL,
	[EQUIPMENT_ID] [int] NULL,
	[PLAN_LOADING] [float] NULL,
	[LOADING] [float] NULL,
	[NOTE] [varchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_MFR_PLANS_JOBS_EQUIPMENTS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EQUIPMENTS]') AND name = N'IX_MFR_PLANS_JOBS_EQUIPMENTS')
CREATE NONCLUSTERED INDEX [IX_MFR_PLANS_JOBS_EQUIPMENTS] ON [MFR_PLANS_JOBS_EQUIPMENTS]
(
	[DETAIL_ID] ASC,
	[EQUIPMENT_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_MFR_PLANS_JOBS_EQUIPMENTS_DETAIL_ID]') AND parent_object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EQUIPMENTS]'))
ALTER TABLE [MFR_PLANS_JOBS_EQUIPMENTS]  WITH CHECK ADD  CONSTRAINT [FK_MFR_PLANS_JOBS_EQUIPMENTS_DETAIL_ID] FOREIGN KEY([DETAIL_ID])
REFERENCES [MFR_PLANS_JOBS_DETAILS] ([ID])
ON DELETE CASCADE
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_MFR_PLANS_JOBS_EQUIPMENTS_DETAIL_ID]') AND parent_object_id = OBJECT_ID(N'[MFR_PLANS_JOBS_EQUIPMENTS]'))
ALTER TABLE [MFR_PLANS_JOBS_EQUIPMENTS] CHECK CONSTRAINT [FK_MFR_PLANS_JOBS_EQUIPMENTS_DETAIL_ID]
GO
/****** Object:  Trigger [tid_mfr_plans_jobs_equipments]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tid_mfr_plans_jobs_equipments]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tid_mfr_plans_jobs_equipments] on [MFR_PLANS_JOBS_EQUIPMENTS]
for insert, update, delete as
begin

	set nocount on;
	if dbo.sys_triggers_enabled() = 0 return -- disabled

	update x
	set 	count_equipments = xx.c_rows
	from mfr_plans_jobs_details x
		left join (
			select detail_id, count(*) as c_rows
			from mfr_plans_jobs_equipments
			group by detail_id
		) xx on xx.detail_id = x.id
	where x.id in (
		select detail_id from inserted
		union select detail_id from deleted
		)
end' 
GO
