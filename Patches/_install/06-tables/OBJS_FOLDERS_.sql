/****** Object:  Table [OBJS_FOLDERS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[OBJS_FOLDERS]') AND type in (N'U'))
BEGIN
CREATE TABLE [OBJS_FOLDERS](
	[FOLDER_ID] [int] IDENTITY(1,1) NOT NULL,
	[KEYWORD] [varchar](32) NOT NULL,
	[NAME] [varchar](128) NULL,
	[STATUS_ID] [int] NOT NULL DEFAULT ((1)),
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NOT NULL,
	[COUNTS] [int] NULL,
	[TOTALS] [varchar](50) NULL,
	[NODE] [hierarchyid] NULL,
	[PARENT_ID] [int] NULL,
	[HAS_CHILDS] [bit] NOT NULL DEFAULT ((0)),
	[SORT_ID] [float] NULL,
	[LEVEL_ID] [int] NULL,
	[IS_DELETED] [bit] NOT NULL DEFAULT ((0)),
	[EXTERN_ID] [int] NULL,
	[SUBJECT_ID] [int] NULL,
	[D_DOC] [datetime] NULL,
	[OBJ_TYPE] [varchar](8) NULL,
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[INHERITED_ACCESS] [bit] NULL,
	[READ_DATE] [datetime] NULL,
	[READ_COUNT] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[FOLDER_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO

/****** Object:  Index [IX_OBJS_FOLDERS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[OBJS_FOLDERS]') AND name = N'IX_OBJS_FOLDERS')
CREATE NONCLUSTERED INDEX [IX_OBJS_FOLDERS] ON [OBJS_FOLDERS]
(
	[NODE] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_OBJS_FOLDERS_EXTERN]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[OBJS_FOLDERS]') AND name = N'IX_OBJS_FOLDERS_EXTERN')
CREATE NONCLUSTERED INDEX [IX_OBJS_FOLDERS_EXTERN] ON [OBJS_FOLDERS]
(
	[SUBJECT_ID] ASC,
	[EXTERN_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Trigger [tiu_objs_folders]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiu_objs_folders]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tiu_objs_folders] on [OBJS_FOLDERS]
for insert, update
as
begin

	set nocount on;

	update x
	set obj_type =
		case
			when x.keyword like ''PAYORDER%'' then ''PO''				
			when x.keyword like ''DOGOVOR%'' then ''DOC''
			else coalesce(o.type, xp.obj_type, x.obj_type)
		end
	from objs_folders x
		left join objs_types o on o.folder_keyword = x.keyword
		left join objs_folders xp on xp.folder_id = x.parent_id
	where x.folder_id in (select folder_id from inserted)
		and x.obj_type is null

end
' 
GO
