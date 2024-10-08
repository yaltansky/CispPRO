/****** Object:  View [DEALS_MAP_ALIASES_BACK]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[DEALS_MAP_ALIASES_BACK]'))
EXEC dbo.sp_executesql @statement = N'CREATE VIEW [DEALS_MAP_ALIASES_BACK]
as
SELECT MA.ALIAS_ID, MA.ALIAS_NAME, MAM.ARTICLE_ID
from deals_map_aliases ma
	join (
		--select isnull(a2.article_id, a.article_id) as article_id, min(alias_id) as alias_id
    select a.article_id, min(alias_id) as alias_id
		from deals_map_aliases ma
			join bdr_articles a on a.article_id = ma.article_id
			--left join bdr_articles a2 on a2.short_name = a.short_name
		group by a.article_id
	) mam on mam.alias_id = ma.alias_id' 
GO
