-- place here scripts which will be processed before installation
IF EXISTS(SELECT 1 FROM SYS.TABLES)
BEGIN
    RAISERROR('Выбранная база данных содержит таблицы. Инсталляция может быть произведена только над пустой базой данных', 16, 1)
END
GO
