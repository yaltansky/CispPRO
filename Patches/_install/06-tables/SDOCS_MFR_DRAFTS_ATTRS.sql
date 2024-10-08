/****** Object:  Table [SDOCS_MFR_DRAFTS_ATTRS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS_ATTRS]') AND type in (N'U'))
BEGIN
CREATE TABLE [SDOCS_MFR_DRAFTS_ATTRS](
	[ID] [bigint] IDENTITY(1,1) NOT NULL,
	[DRAFT_ID] [int] NULL,
	[ATTR_ID] [int] NULL,
	[NOTE] [varchar](max) NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[IS_DELETED] [bit] NULL,
 CONSTRAINT [PK_SDOCS_MFR_DRAFTS_ATTRS] PRIMARY KEY NONCLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_SDOCS_MFR_DRAFTS_ATTRS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_MFR_DRAFTS_ATTRS]') AND name = N'IX_SDOCS_MFR_DRAFTS_ATTRS')
CREATE CLUSTERED INDEX [IX_SDOCS_MFR_DRAFTS_ATTRS] ON [SDOCS_MFR_DRAFTS_ATTRS]
(
	[DRAFT_ID] ASC,
	[ATTR_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

/****** Object:  Trigger [tg_sdocs_mfr_drafts_attrs]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tg_sdocs_mfr_drafts_attrs]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tg_sdocs_mfr_drafts_attrs] on [SDOCS_MFR_DRAFTS_ATTRS]
for insert, update as
begin

	set nocount on;

	declare @узелМассаОбъекта int = (select top 1 attr_id from prodmeta_attrs where name = ''узел.МассаОбъекта'')
	declare @узелРазмер int = (select top 1 attr_id from prodmeta_attrs where name = ''узел.Размер'')

	update x
	set prop_weight = try_parse(a2.note as float),
		prop_size = a3.note
	from sdocs_mfr_drafts x
		left join sdocs_mfr_drafts_attrs a2 on a2.draft_id = x.draft_id and a2.attr_id = @узелМассаОбъекта
		left join sdocs_mfr_drafts_attrs a3 on a3.draft_id = x.draft_id and a3.attr_id = @узелРазмер
	where x.draft_id in (
		select draft_id from inserted where attr_id in (@узелМассаОбъекта,@узелРазмер)
		)

    declare @узелЧертёжПуть int = (select top 1 attr_id from prodmeta_attrs where name = ''узел.ЧертёжПуть'')

    if exists(select 1 from inserted where attr_id = @узелЧертёжПуть)
    begin
        delete x from sdocs_mfr_drafts_docs x
            join inserted i on i.draft_id = x.draft_id and i.attr_id = @узелЧертёжПуть
        where x.number = ''httpref''

        insert into sdocs_mfr_drafts_docs(draft_id, number, name, url)
        select draft_id, ''httpref'', ''Ссылка на чертёж узла/элемента'', note 
        from inserted
        where attr_id = @узелЧертёжПуть
    end

end
' 
GO

/****** Object:  Synonym [MFR_DRAFTS_ATTRS]    Script Date: 9/18/2024 3:28:00 PM ******/
IF NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = N'MFR_DRAFTS_ATTRS' AND schema_id = SCHEMA_ID(N'dbo'))
CREATE SYNONYM [MFR_DRAFTS_ATTRS] FOR [SDOCS_MFR_DRAFTS_ATTRS]
GO
