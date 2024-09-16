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

IF OBJECT_ID('MFR_DOCS_INFOS_STATES') IS NULL
BEGIN
	-- DROP TABLE MFR_DOCS_INFOS_STATES
    CREATE TABLE MFR_DOCS_INFOS_STATES(
		ID INT IDENTITY PRIMARY KEY,	
		INFO_ID INT,
		MFR_DOC_ID INT,
        -- 
        STATE_ID INT,
        NAME VARCHAR(50),
        D_PLAN DATE,
        D_FACT DATE,
        NOTE VARCHAR(MAX),
        -- 
	    INDEX MFR_DOCS_INFOS_STATES(MFR_DOC_ID, INFO_ID)
	)

	-- DROP TABLE MFR_DOCS_INFOS_STATES_REFS
    CREATE TABLE MFR_DOCS_INFOS_STATES_REFS(
        STATE_ID INT IDENTITY PRIMARY KEY,
        NAME VARCHAR(50),
        NOTE VARCHAR(MAX)
	)
    INSERT INTO MFR_DOCS_INFOS_STATES_REFS(NAME)
    VALUES ('Размещение заказа'), ('Разработка КД'), ('Обеспечение материалами'), ('Начало производства'), ('Завершение производства')
END
GO

IF OBJECT_ID('MFR_DOCS_INFOS_MATERIALS') IS NULL
BEGIN
	-- DROP TABLE MFR_DOCS_INFOS_MATERIALS
    CREATE TABLE MFR_DOCS_INFOS_MATERIALS(
		ID INT IDENTITY PRIMARY KEY,	
		INFO_ID INT,
		MFR_DOC_ID INT,
        -- 
        SLICE VARCHAR(20),
		ITEM_ID INT,
        D_TO_PLAN DATE,
        D_TO_FACT DATE,
        DURATION INT,
        -- 
	    INDEX MFR_DOCS_INFOS_MATERIALS(INFO_ID, SLICE, ITEM_ID)
	)
END
GO

IF OBJECT_ID('MFR_DOCS_INFOS_SYNCS') IS NULL
BEGIN
	-- DROP TABLE MFR_DOCS_INFOS_SYNCS
    CREATE TABLE MFR_DOCS_INFOS_SYNCS(
		ID INT IDENTITY PRIMARY KEY,	
		INFO_ID INT,
		MFR_DOC_ID INT,
        -- 
		CONTENT_ID INT,
        NAME VARCHAR(500),
		DELAY INT,
        -- 
	    INDEX IX_MFR_DOCS_INFOS_SYNCS(MFR_DOC_ID, INFO_ID)
	)
END
GO

IF OBJECT_ID('MFR_DOCS_INFOS_HISTS') IS NULL
BEGIN
	-- DROP TABLE MFR_DOCS_INFOS_HISTS
    CREATE TABLE MFR_DOCS_INFOS_HISTS(
		ID INT IDENTITY PRIMARY KEY,	
		MFR_DOC_ID INT,
        NOTE VARCHAR(MAX),
		ADD_DATE DATETIME DEFAULT GETDATE(),
		ADD_MOL_ID INT,
	)
END
GO

