/****** Object:  Table [TASKS_MOLS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[TASKS_MOLS]') AND type in (N'U'))
BEGIN
CREATE TABLE [TASKS_MOLS](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[TASK_ID] [int] NOT NULL,
	[ROLE_ID] [int] NULL DEFAULT ((1)),
	[MOL_ID] [int] NOT NULL,
	[DURATION] [int] NULL,
	[D_DEADLINE] [datetime] NULL,
	[D_EXECUTED] [datetime] NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[IS_FAVORITE] [bit] NULL,
	[STATUS_ID] [int] NULL,
	[D_LAST_OPENED] [datetime] NULL,
	[PRIORITY_ID] [int] NULL,
	[SLICE] [varchar](20) NULL,
	[ATTRS] [varchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

/****** Object:  Index [IX_TASKS_MOLS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[TASKS_MOLS]') AND name = N'IX_TASKS_MOLS')
CREATE UNIQUE NONCLUSTERED INDEX [IX_TASKS_MOLS] ON [TASKS_MOLS]
(
	[TASK_ID] ASC,
	[MOL_ID] ASC,
	[ROLE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_TASKS_MOLS_ROLE_MOL]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[TASKS_MOLS]') AND name = N'IX_TASKS_MOLS_ROLE_MOL')
CREATE NONCLUSTERED INDEX [IX_TASKS_MOLS_ROLE_MOL] ON [TASKS_MOLS]
(
	[ROLE_ID] ASC,
	[MOL_ID] ASC
)
INCLUDE ( 	[TASK_ID],
	[STATUS_ID]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_TASKS_MOLS_TASKS]') AND parent_object_id = OBJECT_ID(N'[TASKS_MOLS]'))
ALTER TABLE [TASKS_MOLS]  WITH CHECK ADD  CONSTRAINT [FK_TASKS_MOLS_TASKS] FOREIGN KEY([TASK_ID])
REFERENCES [TASKS] ([TASK_ID])
ON DELETE CASCADE
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_TASKS_MOLS_TASKS]') AND parent_object_id = OBJECT_ID(N'[TASKS_MOLS]'))
ALTER TABLE [TASKS_MOLS] CHECK CONSTRAINT [FK_TASKS_MOLS_TASKS]
GO
/****** Object:  Trigger [tu_tasks_mols_deadline]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tu_tasks_mols_deadline]'))
EXEC dbo.sp_executesql @statement = N'
create trigger [tu_tasks_mols_deadline] on [TASKS_MOLS]
for update
as begin
 
	set nocount on;
	
	if update(d_deadline)
	begin
		update tasks
		set d_deadline_analyzer = (select max(d_deadline) from tasks_mols where task_id = tasks.task_id and role_id = 1)			
		where task_id in (select distinct task_id from inserted)
	end

end
' 
GO
/****** Object:  Trigger [tu_tasks_mols_status]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tu_tasks_mols_status]'))
EXEC dbo.sp_executesql @statement = N'
create trigger [tu_tasks_mols_status] on [TASKS_MOLS]
for update
as begin
 
	set nocount on;
	
	if update(d_executed)
	begin
		update x
		set status_id = case when d_executed is not null then 5 else t.status_id end
		from tasks_mols x
			inner join tasks t on t.task_id = x.task_id
		where id in (select distinct id from inserted)
	end

end' 
GO
