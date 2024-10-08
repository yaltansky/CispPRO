/****** Object:  Table [AGENTS_GROUPS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[AGENTS_GROUPS]') AND type in (N'U'))
BEGIN
CREATE TABLE [AGENTS_GROUPS](
	[SUBJECT_ID] [int] NOT NULL DEFAULT ((-2)),
	[AGENT_ID] [int] NOT NULL,
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[GROUP_ID] [int] NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[ADD_MOL_ID] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Trigger [tiud_agents_groups]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiud_agents_groups]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tiud_agents_groups] on [AGENTS_GROUPS]
for insert, update, delete as
begin
	
	set nocount on;

	declare @groups_count int, @group_name varchar(100)

	update x
	set @group_name = left(
			(
			select cast(gn.name as varchar) + ''; '' as [text()]
			from agents_groups gr
				join agents_groups_names gn on gn.group_id = gr.group_id
			where gr.agent_id = x.agent_id
			for xml path('''')
			), 100),
		group_name = 
			case
				when len(@group_name) > 1 then left(@group_name, len(@group_name) - 1)
			end
	from agents x
	where x.agent_id in (select agent_id from inserted union select agent_id from deleted)

end' 
GO
