/****** Object:  Table [MFR_ITEMS_PRICES]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MFR_ITEMS_PRICES]') AND type in (N'U'))
BEGIN
CREATE TABLE [MFR_ITEMS_PRICES](
	[PRODUCT_ID] [int] NOT NULL,
	[D_LAST] [date] NULL,
	[UNIT_ID] [int] NULL,
	[PRICE_PURE] [float] NULL,
	[PRICE] [float] NULL,
	[D_CALC] [datetime] NOT NULL DEFAULT (getdate()),
PRIMARY KEY CLUSTERED 
(
	[PRODUCT_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO
