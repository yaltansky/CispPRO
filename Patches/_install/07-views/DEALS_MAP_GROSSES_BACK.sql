/****** Object:  View [DEALS_MAP_GROSSES_BACK]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[DEALS_MAP_GROSSES_BACK]'))
EXEC dbo.sp_executesql @statement = N'CREATE VIEW [DEALS_MAP_GROSSES_BACK]
as
SELECT MA.GROSS_ID, MA.GROSS_NAME, MAM.ARTICLE_ID
from deals_map_grosses ma
	join (
		select isnull(a2.article_id, a.article_id) as article_id, min(gross_id) as gross_id
		from deals_map_grosses ma
			join bdr_articles a on a.article_id = ma.article_id
				left join bdr_articles a2 on a2.short_name = a.short_name
		group by isnull(a2.article_id, a.article_id)
	) mam on mam.gross_id = ma.gross_id' 
GO
