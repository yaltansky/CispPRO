-- dbcontext: CISP
DELETE FROM OBJS_META_REFS WHERE FROM_OBJ_TYPE IN (
    'MFR', 'MCO', 'MFC', 'MFD', 'MFJ', 'MFM', 'MFO', 'MFP', 'MFTRF', 
    'MFW', 'MFWD',
    'SWP', 'MFPDM'
    )

INSERT INTO OBJS_META_REFS(FROM_OBJ_TYPE, TO_OBJ_TYPE)
VALUES
	('MFR', 'MFC'), ('MFR', 'MFM')	

INSERT INTO OBJS_META_REFS(FROM_OBJ_TYPE, TO_OBJ_TYPE, TO_OBJ_NAME, URL)
VALUES
	-- Планы
	('MFP', 'P', 'Готовая продукция', null),
	('MFP', 'MFR', 'Пр.заказы', null),

	-- Тех.выписки
	('MFD', 'MFC', null, null),
	('MFD', 'MFD-all', 'Все тех.выписки', '#self'),
	('MFD', 'MFD-unique', 'Найти уникальные', '#self'),
	
	-- Детали
	('MFC', 'MFR', null, null),
	('MFC', 'MFD', null, null),
	('MFC', 'MFC-parents', 'Родительские детали', null),
	('MFC', 'MFC-parentsall', 'Родительские детали (все)', null),
	('MFC', 'MFC-parentsonly', 'Оставить верхний уровень', null),
	('MFC', 'MFC-childs', 'Дочерние детали', null),
	('MFC', 'MFM', null, null),
	('MFC', 'MFJ', null, null),
	('MFC', 'MCO', null, null), -- очередь заданий
	('MFC', 'MFO', null, null),	
	('MFC', 'P', 'Готовая продукция', null),
	('MFC', 'P2', 'Товарные позиции', null),
	('MFC', 'INV', 'Счета (кооперация)', null),
	('MFC', 'INVPAY', 'Счета и оплаты (кооперация)', null),

	-- Материалы
	('MFM', 'MFR', null, null),	
	('MFM', 'MFM-parents', 'Детали материалов', null),
	('MFM', 'MFM-siblings', 'Смежные материалы', null),
	('MFM', 'BUYORDER', null, null),
	('MFM', 'INV', null, null),
	('MFM', 'INVPAY', null, null),
	('MFM', 'SHIP', 'Приходы на склад', '/sdocs'),
	('MFM', 'MFTRF', 'Выдача в производство', null),
	('MFM', 'SWP', null, null),
	('MFM', 'P2', 'Товарные позиции', null),

    -- Библиотека
    ('MFPDM', 'P', null, null),	
    ('MFPDM', 'childs', 'Подчинённые части', '#self'),	
    ('MFPDM', 'parents', 'Вхождения', '#self'),	

	-- Замены материалов
	('SWP', 'MFM', 'Материалы', null),
	('SWP', 'P', null, null),

	-- Операции
	('MFO', 'MFC', null, null),

	-- Передаточные накладные
	('MFTRF', 'MFM', null, null),
	('MFTRF', 'P', null, null),

	-- Сменные задания
	('MFJ', 'MFC', null, null),
	('MFJ', 'MCO', null, null),
	('MFJ', 'MFW', null, null),

	-- Очередь заданий
	('MCO', 'MFJ', null, null),
	('MCO', 'MFC', null, null),
	('MCO', 'MCO-next', 'Следующие операции', '#self'),

	-- Табели рабочего времени
	('MFW', 'MFJ', null, null),

	-- Сменные задания
	('MFWD', 'MFW', null, null),
	('MFWD', 'MFJ', null, null)
