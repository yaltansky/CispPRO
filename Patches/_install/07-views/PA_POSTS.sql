/****** Object:  View [PA_POSTS]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[PA_POSTS]'))
EXEC dbo.sp_executesql @statement = N'CREATE VIEW [PA_POSTS]
AS SELECT POST_ID, NAME, SUBJECT_ID, IS_DELETED FROM MOLS_POSTS
' 
GO
