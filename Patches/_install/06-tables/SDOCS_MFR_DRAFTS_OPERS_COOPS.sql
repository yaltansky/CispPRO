/****** Object:  Table [SDOCS_MFR_DRAFTS_OPERS_COOPS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS_OPERS_COOPS]') AND type in (N'U'))
BEGIN
CREATE TABLE [SDOCS_MFR_DRAFTS_OPERS_COOPS](
	[ID] [bigint] IDENTITY(1,1) NOT NULL,
	[DRAFT_ID] [int] NULL,
	[OPER_ID] [int] NULL,
	[ITEM_ID] [int] NULL,
	[UNIT_ID] [int] NULL,
	[QUANTITY] [float] NULL,
	[SUM_PRICE] [float] NULL,
	[SUM_VALUE] [float] NULL,
	[NOTE] [varchar](max) NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[IS_DELETED] [bit] NULL,
PRIMARY KEY NONCLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_SDOCS_MFR_DRAFTS_OPERS_COOPS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS_OPERS_COOPS]') AND name = N'IX_SDOCS_MFR_DRAFTS_OPERS_COOPS')
CREATE CLUSTERED INDEX [IX_SDOCS_MFR_DRAFTS_OPERS_COOPS] ON [SDOCS_MFR_DRAFTS_OPERS_COOPS]
(
	[DRAFT_ID] ASC,
	[OPER_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Trigger [tg_sdocs_mfr_drafts_opers_coops]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tg_sdocs_mfr_drafts_opers_coops]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tg_sdocs_mfr_drafts_opers_coops] on [SDOCS_MFR_DRAFTS_OPERS_COOPS]
for insert, update, delete as
begin

	set nocount on;
	if dbo.sys_triggers_enabled() = 0 return -- disabled

	update x
	set count_resources = xx.c_rows
	from sdocs_mfr_drafts_opers x
		left join (
			select oper_id, count(*) as c_rows
			from sdocs_mfr_drafts_opers_coops
			group by oper_id
		) xx on xx.oper_id = x.oper_id
	where x.oper_id in (
		select oper_id from inserted
		union select oper_id from deleted
		)
end' 
GO
