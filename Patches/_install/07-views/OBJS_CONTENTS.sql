/****** Object:  View [OBJS_CONTENTS]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[OBJS_CONTENTS]'))
EXEC dbo.sp_executesql @statement = N'
CREATE VIEW [OBJS_CONTENTS]
as

select 
	OBJ_UID,
	OWNER_TYPE,
	OWNER_ID,
	OWNER_NAME,
    CONTENT
from OBJS
' 
GO
