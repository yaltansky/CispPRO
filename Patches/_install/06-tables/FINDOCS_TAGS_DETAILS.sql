/****** Object:  Table [FINDOCS_TAGS_DETAILS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[FINDOCS_TAGS_DETAILS]') AND type in (N'U'))
BEGIN
CREATE TABLE [FINDOCS_TAGS_DETAILS](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[TAG_ID] [int] NOT NULL,
	[FINDOC_ID] [int] NOT NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Index [IX_FINDOCS_TAGS_DETAILS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[FINDOCS_TAGS_DETAILS]') AND name = N'IX_FINDOCS_TAGS_DETAILS')
CREATE UNIQUE NONCLUSTERED INDEX [IX_FINDOCS_TAGS_DETAILS] ON [FINDOCS_TAGS_DETAILS]
(
	[TAG_ID] ASC,
	[FINDOC_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Trigger [tiud_findocs_tags_details]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiud_findocs_tags_details]'))
EXEC dbo.sp_executesql @statement = N'
create trigger [tiud_findocs_tags_details] on [FINDOCS_TAGS_DETAILS]
for insert, update, delete
as
begin

	set nocount on;

	update fd
	set has_tags = case when exists(select 1 from findocs_tags_details where findoc_id = fd.findoc_id) then 1 else 0 end
	from findocs fd
	where fd.findoc_id in (
		select findoc_id from inserted union all select findoc_id from deleted
		)
	
end
' 
GO
