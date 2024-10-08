/****** Object:  Table [SDOCS_PRODUCTS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[SDOCS_PRODUCTS]') AND type in (N'U'))
BEGIN
CREATE TABLE [SDOCS_PRODUCTS](
	[DETAIL_ID] [int] IDENTITY(1,1) NOT NULL,
	[DOC_ID] [int] NULL,
	[PRODUCT_ID] [int] NULL,
	[QUANTITY] [float] NULL,
	[W_NETTO] [float] NULL,
	[W_BRUTTO] [float] NULL,
	[UNIT_ID] [int] NULL,
	[PRICE] [float] NULL,
	[PRICE_PURE] [float] NULL,
	[PRICE_PURE_TRF] [float] NULL,
	[NDS_RATIO] [float] NULL,
	[VALUE_PURE] [decimal](18, 2) NULL,
	[VALUE_NDS] [decimal](18, 2) NULL,
	[VALUE_CCY] [decimal](18, 2) NULL,
	[VALUE_RUR] [decimal](18, 2) NULL,
	[NOTE] [varchar](max) NULL,
	[REFKEY] [varchar](100) NULL,
	[VALUE_WORK] [decimal](18, 2) NULL,
	[PRICE_LIST] [decimal](18, 2) NULL,
	[MFR_NUMBER] [varchar](50) NULL,
	[DEST_PRODUCT_ID] [int] NULL,
	[PLAN_Q] [float] NULL,
	[DUE_DATE] [date] NULL,
	[DEST_UNIT_ID] [int] NULL,
	[DEST_QUANTITY] [float] NULL,
	[MFR_NUMBER_FROM] [varchar](100) NULL,
	[DRAFT_ID] [int] NULL,
	[NUMPOS] [int] NULL,
	[ERRORS] [varchar](max) NULL,
	[HAS_DETAILS] [bit] NULL,
PRIMARY KEY CLUSTERED 
(
	[DETAIL_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
)
END
GO
/****** Object:  Index [IX_SDOCS_PRODUCTS_MFR_NUMBER]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_PRODUCTS]') AND name = N'IX_SDOCS_PRODUCTS_MFR_NUMBER')
CREATE NONCLUSTERED INDEX [IX_SDOCS_PRODUCTS_MFR_NUMBER] ON [SDOCS_PRODUCTS]
(
	[MFR_NUMBER] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
/****** Object:  Index [IX_SDOCS_PRODUCTS_PRODUCT_ID]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[SDOCS_PRODUCTS]') AND name = N'IX_SDOCS_PRODUCTS_PRODUCT_ID')
CREATE NONCLUSTERED INDEX [IX_SDOCS_PRODUCTS_PRODUCT_ID] ON [SDOCS_PRODUCTS]
(
	[DOC_ID] ASC,
	[PRODUCT_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_SDOCS_PRODUCTS_DOC_ID]') AND parent_object_id = OBJECT_ID(N'[SDOCS_PRODUCTS]'))
ALTER TABLE [SDOCS_PRODUCTS]  WITH CHECK ADD  CONSTRAINT [FK_SDOCS_PRODUCTS_DOC_ID] FOREIGN KEY([DOC_ID])
REFERENCES [SDOCS] ([DOC_ID])
ON DELETE CASCADE
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[FK_SDOCS_PRODUCTS_DOC_ID]') AND parent_object_id = OBJECT_ID(N'[SDOCS_PRODUCTS]'))
ALTER TABLE [SDOCS_PRODUCTS] CHECK CONSTRAINT [FK_SDOCS_PRODUCTS_DOC_ID]
GO
/****** Object:  Trigger [tiu_sdocs_products]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[tiu_sdocs_products]'))
EXEC dbo.sp_executesql @statement = N'create trigger [tiu_sdocs_products] on [SDOCS_PRODUCTS]
for insert, update as
begin
	
	if dbo.sys_triggers_enabled() = 0 return -- disabled

	set nocount on;

	declare @price float, @value_pure float, @value_rur float

	update x set
		@price = x.price_pure * (1 + x.nds_ratio),
		@value_pure = x.price_pure * x.quantity,
		@value_rur = @price * x.quantity,
		price = @price,
		value_pure = @value_pure,
		value_nds = @value_rur - @value_pure,
		value_rur = @value_rur,
		value_ccy = @value_rur
	from sdocs_products x
		join inserted i on i.detail_id = x.detail_id
	where x.price_pure > 0
	
	update x set
		value_ccy = p.value_ccy,
		value_rur = p.value_rur
	from sdocs x
		join (
			select doc_id, sum(value_ccy) as value_ccy, sum(value_rur) as value_rur
			from sdocs_products
			group by doc_id
		) p on p.doc_id = x.doc_id
	where x.doc_id in (select distinct doc_id from inserted)

end' 
GO
