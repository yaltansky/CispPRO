/****** Object:  Table [OBJS_VIEWS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[OBJS_VIEWS]') AND type in (N'U'))
BEGIN
CREATE TABLE [OBJS_VIEWS](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[OBJ_TYPE] [varchar](3) NULL,
	[OBJ_ID] [int] NULL,
	[MOL_ID] [int] NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Index [IX_OBJS_VIEWS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[OBJS_VIEWS]') AND name = N'IX_OBJS_VIEWS')
CREATE NONCLUSTERED INDEX [IX_OBJS_VIEWS] ON [OBJS_VIEWS]
(
	[OBJ_TYPE] ASC,
	[OBJ_ID] ASC,
	[MOL_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

/****** Object:  Trigger [ti_objs_views]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[ti_objs_views]'))
EXEC dbo.sp_executesql @statement = N'
create trigger [ti_objs_views] on [OBJS_VIEWS]
for insert as
begin
	
	set nocount on;

	update x
	set c_views = (select count(*) from objs_views where obj_type = ''prr'' and obj_id = x.result_id)
	from projects_results x
	where exists(select 1 from inserted where obj_type = ''prr'' and obj_id = x.result_id)

end' 
GO
