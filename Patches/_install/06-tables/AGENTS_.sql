/****** Object:  Table [AGENTS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[AGENTS]') AND type in (N'U'))
BEGIN
CREATE TABLE [AGENTS](
	[AGENT_ID] [int] IDENTITY(1,1) NOT NULL,
	[NAME] [varchar](255) NOT NULL,
	[INN] [varchar](30) NULL,
	[IS_DELETED] [bit] NOT NULL DEFAULT ((0)),
	[ADDRESS] [nvarchar](max) NULL,
	[CATEGORY_ID] [int] NOT NULL DEFAULT ((0)),
	[JURADDR] [nvarchar](max) NULL,
	[NAME_PRINT] [nvarchar](max) NULL,
	[MAIN_ID] [int] NULL,
	[STATUS_ID] [int] NOT NULL DEFAULT ((0)),
	[COUNTRY_ID] [int] NULL,
	[KPP] [varchar](50) NULL,
	[WEBSITE] [varchar](max) NULL,
	[PARENT_COMPANY_ID] [int] NULL,
	[ADMIN_ID] [int] NULL,
	[GROUP_NAME] [varchar](100) NULL,
	[PHONE] [varchar](500) NULL,
	[FAX] [varchar](250) NULL,
	[EMAIL] [varchar](100) NULL,
	[ADD_MOL_ID] [int] NULL,
	[ADD_DATE] [datetime] DEFAULT getdate(),
	[UPDATE_DATE] [datetime] NULL,
	[UPDATE_MOL_ID] [int] NULL,
	[EXTERNAL_ID] [int] NULL,
	[OGRN] [varchar](100) NULL,
	[MANAGER_NAME] [varchar](100) NULL,
	[MANAGER_POST] [varchar](150) NULL,
	[ADDR_COUNTRY] [varchar](50) NULL,
	[ADDR_REGION] [varchar](50) NULL,
	[ADDR_CITY] [varchar](50) NULL,
	[ADDR_GEOLAT] [varchar](50) NULL,
	[ADDR_GEOLON] [varchar](50) NULL,
	[DADATA_HID] [varchar](100) NULL,
	[DD_BRANCH] [varchar](50) NULL,
	[DD_TYPE] [varchar](50) NULL,
	[DD_STATE] [varchar](100) NULL,
	[DD_STATE_REGISTRATION] [varchar](10) NULL,
	[DD_STATE_LIQUIDATION] [varchar](10) NULL,
	[DD_STATE_ACTUALITY] [varchar](10) NULL,
	[GROUP_ID] [int] NULL,
	[OKPO] [varchar](100) NULL,
 CONSTRAINT [PK__AGENTS__0BB8E2B146D5246C] PRIMARY KEY CLUSTERED 
(
	[AGENT_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_AGENTS_AGENTS_PARENT_COMPANY_ID]') AND parent_object_id = OBJECT_ID(N'[AGENTS]'))
ALTER TABLE [AGENTS]  WITH CHECK ADD  CONSTRAINT [FK_AGENTS_AGENTS_PARENT_COMPANY_ID] FOREIGN KEY([PARENT_COMPANY_ID])
REFERENCES [AGENTS] ([AGENT_ID])
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_AGENTS_AGENTS_PARENT_COMPANY_ID]') AND parent_object_id = OBJECT_ID(N'[AGENTS]'))
ALTER TABLE [AGENTS] CHECK CONSTRAINT [FK_AGENTS_AGENTS_PARENT_COMPANY_ID]
GO
/****** Object:  Trigger [ti_agents_log]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[ti_agents_log]'))
EXEC dbo.sp_executesql @statement = N'create trigger [ti_agents_log] on [AGENTS]
for insert as
begin
	
	set nocount on;

	declare @dbcc varchar(50) = ''dbcc inputbuffer('' + str(@@spid) + '')''
   	declare @inputbuffer table(EventType nvarchar(30), Parameters int, EventInfo varchar(max))
   	insert into @inputbuffer exec(@dbcc)
    
	declare 
		@tran_id uniqueidentifier = newid(),
		@tran_caller varchar(max) = (select top 1 eventinfo from @inputbuffer),
		@tran_user_id int = dbo.sys_user_id(),
		@tran_action char(1) = 	
			case
				when exists(select 1 from inserted) and exists(select 1 from deleted) then ''U''
				when exists(select 1 from inserted) then ''I''
				else ''D''
			end

	insert into agents_log(
		tran_id, tran_caller, tran_action, tran_user_id,
		agent_id, name, inn
		)
	select 
		@tran_id, @tran_caller, @tran_action, @tran_user_id,
		*
	from (
		select agent_id, name, inn
		from inserted where @tran_action in (''I'')
		) u

end
' 
GO
