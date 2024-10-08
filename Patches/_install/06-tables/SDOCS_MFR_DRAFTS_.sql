/****** Object:  Table [SDOCS_MFR_DRAFTS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS]') AND type in (N'U'))
BEGIN
CREATE TABLE [SDOCS_MFR_DRAFTS](
	[DRAFT_ID] [int] IDENTITY(1,1) NOT NULL,
	[PLAN_ID] [int] NULL,
	[MFR_DOC_ID] [int] NULL,
	[PRODUCT_ID] [int] NULL,
	[ITEM_ID] [int] NULL,
	[IS_BUY] [bit] NOT NULL DEFAULT ((0)),
	[STATUS_ID] [int] NULL,
	[NUMBER] [varchar](50) NULL,
	[D_DOC] [datetime] NULL,
	[MOL_ID] [int] NULL,
	[NOTE] [varchar](max) NULL,
	[ITEM_PRICE0] [decimal](18, 2) NULL,
	[OPERS_COUNT] [int] NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[IS_DELETED] [bit] NOT NULL DEFAULT ((0)),
	[CONTEXT] [varchar](20) NULL,
	[MAIN_ID] [int] NULL,
	[EXECUTOR_ID] [int] NULL,
	[ITEM_IMG] [varchar](max) NULL,
	[PROP_WEIGHT] [float] NULL,
	[PROP_SIZE] [varchar](50) NULL,
	[IS_ROOT] [bit] NOT NULL DEFAULT ((0)),
	[RESERVED] [int] NULL,
	[CHKSUM] [int] NULL,
	[PART_Q] [float] NULL,
	[TEMPLATE_ID] [int] NULL,
	[TYPE_ID] [int] NOT NULL DEFAULT ((1)),
	[WORK_TYPE_1] [bit] NULL,
	[WORK_TYPE_2] [bit] NULL,
	[WORK_TYPE_3] [bit] NULL,
	[IS_DESIGN] [bit] NOT NULL DEFAULT ((0)),
	[SOURCE_ID] [int] NULL,
	[UNIT_NAME] [varchar](20) NULL,
	[EXTERN_ID] [varchar](32) NULL,
	[IS_PRODUCT] [bit] NULL,
	[PDM_ID] [int] NULL,
 CONSTRAINT [PK_SDOCS_MFR_DRAFTS] PRIMARY KEY CLUSTERED 
(
	[DRAFT_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_SDOCS_MFR_DRAFTS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS]') AND name = N'IX_SDOCS_MFR_DRAFTS')
CREATE NONCLUSTERED INDEX [IX_SDOCS_MFR_DRAFTS] ON [SDOCS_MFR_DRAFTS]
(
	[MFR_DOC_ID] ASC,
	[PRODUCT_ID] ASC,
	[TYPE_ID] ASC,
	[ITEM_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Trigger [tg_sdocs_mfr_drafts]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tg_sdocs_mfr_drafts]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tg_sdocs_mfr_drafts] on [SDOCS_MFR_DRAFTS]
for insert, update as
begin

	set nocount on;

	if update(is_buy)
		update x
		set work_type_1 = case when x.is_buy = 0 then 1 else 0 end,
			work_type_2 = case when x.is_buy = 1 then 1 else 0 end
		from sdocs_mfr_drafts x
			join inserted i on i.draft_id = x.draft_id

	if update(status_id)
		update x
		set is_deleted = case when x.status_id = -1 then 1 else 0 end
		from sdocs_mfr_drafts x
			join inserted i on i.draft_id = x.draft_id

	else if update(is_deleted)
		update x
		set status_id = 
				case 
					when x.is_deleted = 1 then -1 
					else (case when x.status_id = -1 then 0 else x.status_id end)
				end
		from sdocs_mfr_drafts x
			join inserted i on i.draft_id = x.draft_id

	if dbo.sys_triggers_enabled() = 0 return -- disabled

    if update(item_id)
    begin
        update x set pdm_id = case when x.item_id = pd.item_id then x.pdm_id end
        from mfr_drafts x
            join inserted i on i.draft_id = x.draft_id
            join mfr_pdms pd on pd.pdm_id = x.pdm_id
        
        delete x from mfr_drafts_pdm x
            join inserted i on i.draft_id = x.draft_id
        where i.pdm_id is null
    end

	declare @context varchar(50)	

	if update(mfr_doc_id)
		update x
		set product_id = (select top 1 product_id from sdocs_products where doc_id = x.mfr_doc_id)
		from sdocs_mfr_drafts x
			join inserted i on i.draft_id = x.draft_id
		where x.product_id is null

	update x
	set d_doc = isnull(x.d_doc, dbo.getday(x.add_date)),
		context = 
			case
				when x.main_id is not null then xx.context -- inherits from main
				when x.plan_id = 0 then ''shared''
				when x.plan_id is not null then ''plan''
				else isnull(x.context, ''source'')
			end
	from sdocs_mfr_drafts x
		left join sdocs_mfr_drafts xx on xx.draft_id = x.main_id
	where x.draft_id in (
		select draft_id from inserted
		union select draft_id from deleted
		)

	-- -- sync_dirty
	-- 	update x set sync_dirty = 1
	-- 	from sdocs x
	-- 	where doc_id in (select mfr_doc_id from inserted)
	-- 		and exists(select 1 from sdocs where parent_id = x.doc_id)

end
' 
GO
