/****** Object:  View [dhx_gantt_links]    Script Date: 9/18/2024 3:26:25 PM ******/
IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dhx_gantt_links]'))
EXEC dbo.sp_executesql @statement = N'
create view [dhx_gantt_links]
as

select
	project_id,
	link_id as id,
	source_id as source,
	target_id as target,
	type_id as type
from projects_tasks_links
' 
GO
