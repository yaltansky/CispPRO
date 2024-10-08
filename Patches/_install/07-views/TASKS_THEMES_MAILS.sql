/****** Object:  View [TASKS_THEMES_MAILS]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[TASKS_THEMES_MAILS]'))
EXEC dbo.sp_executesql @statement = N'
CREATE VIEW [TASKS_THEMES_MAILS]
as

select 
	THEME_ID,
	NAME,
    MAIL_HOST,
    MAIL_PORT,
    MAIL_USESSL,
    MAIL_USERNAME,
    MAIL_PASSWORD
from tasks_themes
where isnull(mail_host,'''') <> ''''
	and isnull(mail_username,'''') <> ''''
	and isnull(mail_password,'''') <> ''''

' 
GO
