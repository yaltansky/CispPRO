USE [CISP_SHARED]
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DEALS_CREDITS_MOVES_TYPES]') AND type in (N'U'))
BEGIN
CREATE TABLE [DEALS_CREDITS_MOVES_TYPES](
	[MOVE_TYPE_ID] [int] NOT NULL,
	[NAME] [varchar](20) NULL,
 CONSTRAINT [PK_DEALS_CREDITS_MOVES_TYPES] PRIMARY KEY CLUSTERED 
(
	[MOVE_TYPE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
END
GO
