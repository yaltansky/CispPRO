USE master

declare @kill varchar(max) = ''
    select @kill = @kill + 'kill ' + convert(varchar(5), session_id) + ';'  
    from sys.dm_exec_sessions
    where database_id  = db_id('CISPPRO')
exec(@kill);
GO

if not exists(select 1 from master.sys.sql_logins where name = 'cisp')
    create login cisp with password = 'cisp1234'
GO

DROP DATABASE CISPPRO
GO
CREATE DATABASE CISPPRO
GO

USE CISPPRO
create user cisp for login cisp;
alter role db_owner add member cisp
go

-- CISP_SHARED
declare @kill varchar(max) = ''
    select @kill = @kill + 'kill ' + convert(varchar(5), session_id) + ';'  
    from sys.dm_exec_sessions
    where database_id  = db_id('CISP_SHARED')
exec(@kill);
GO

DROP DATABASE CISP_SHARED
GO
CREATE DATABASE CISP_SHARED
GO

USE CISP_SHARED
create user cisp for login cisp;
alter role db_owner add member cisp
go
