/****** Object:  View [PA_EMPLOYEES_INFO]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[PA_EMPLOYEES_INFO]'))
EXEC dbo.sp_executesql @statement = N'CREATE VIEW [PA_EMPLOYEES_INFO] AS 
	SELECT EMPLOYEE_ID, SUBJECT_ID, PERSON_ID, PERSON_ENTITY_ID, STAFF_POSITION_ID,
		NAME, PHONE, PHONE_LOCAL, PHONE_MOBILE, ROOM
	FROM PA_EMPLOYEES
' 
GO
