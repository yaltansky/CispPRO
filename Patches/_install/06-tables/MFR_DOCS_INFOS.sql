/****** Object:  Table [MFR_DOCS_INFOS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF OBJECT_ID('MFR_DOCS_INFOS') IS NULL
BEGIN
	-- DROP TABLE MFR_DOCS_INFOS
    CREATE TABLE MFR_DOCS_INFOS(
		INFO_ID INT IDENTITY PRIMARY KEY,
		MFR_DOC_ID INT,
        IS_LAST BIT NOT NULL DEFAULT(0),
        -- 
        MX_PV FLOAT, -- Плановый объём (PV): стоимость услуг из товарной части заказа
        MX_EV FLOAT, -- Освоенный объём (EV): Трудоёмкость по завершённым операциям x Стоимость часа
        MX_WK_HOURS FLOAT, -- Общая трудоёмкость, ч
        MX_WK_COST FLOAT, -- Стоимость часа, руб/ч = Стоимость услуг / Общая трудоёмкость
        MX_SPI FLOAT, -- Индекс выполнения плана (EV / PV)
        -- 
        MAT_K_PROVIDED FLOAT, -- Обеспечение, %
		MAT_DELAYS INT, -- Отставание, дн
		MAT_DELAYS_CURRENT INT, -- Текущее отставание, дн
		MAT_DURATION INT, -- Максимальный срок закупки, дн
		MAT_D_FROM_PLAN DATE, -- От (ПДО)
		MAT_D_TO_PLAN DATE, -- До (ПДО)
		MAT_D_TO_FACT DATE, -- До (факт)
        -- 
        PROD_K_COMPLETED FLOAT, -- Исполнение, % (по освоенной трудоёмкости)
		PROD_DELAYS INT, -- Отставание, дн
		PROD_DURATION INT, -- Длительность цикла, дн
		PROD_D_FROM_PLAN DATE, -- От (ПДО)
		PROD_D_TO_PLAN DATE, -- До (ПДО)
		PROD_D_TO_FACT DATE, -- До (факт)
        -- 
		CONT_DOWNTIME INT, -- Максимальный простой, дн: по сделанным операциям
		CONT_DOWNTIME_CURRENT INT, -- Максимальный текущий простой, дн: по текущим операциям (статусы: Готов к выдаче, В работе) и = max(Текущая дата - Дата завершения предыдущей операции)
		CONT_EXEC_DELAY INT, -- Затягивание исполнения, дн: по операциям “Есть назначения” и = (Текущая дата - Дата назначения) - Длительность операции
        -- 
		ADD_DATE DATETIME DEFAULT GETDATE(),
		ADD_MOL_ID INT,
        -- 
	    INDEX MFR_DOCS_INFOS(MFR_DOC_ID, INFO_ID)
	)
END
GO
/****** Object:  Index [MFR_DOCS_INFOS]    Script Date: 9/18/2024 3:24:46 PM ******/
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[MFR_DOCS_INFOS]') AND name = N'MFR_DOCS_INFOS')
CREATE NONCLUSTERED INDEX [MFR_DOCS_INFOS] ON [MFR_DOCS_INFOS]
(
	[MFR_DOC_ID] ASC,
	[INFO_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
