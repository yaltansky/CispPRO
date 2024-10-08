if object_id('strip_html') is not null drop function strip_html
GO
create function [strip_html] (@htmltext varchar(max))
returns varchar(max) as
begin
    declare @start int
    declare @end int
    declare @length int
    set @start = charindex('<',@htmltext)
    set @end = charindex('>',@htmltext,charindex('<',@htmltext))
    set @length = (@end - @start) + 1
    while @start > 0 and @end > 0 and @length > 0
    begin
        set @htmltext = stuff(@htmltext,@start,@length,'')
        set @start = charindex('<',@htmltext)
        set @end = charindex('>',@htmltext,charindex('<',@htmltext))
        set @length = (@end - @start) + 1
    end
    while @htmltext like '%  %'
    begin
        set @htmltext = replace(@htmltext,'  ', ' ')
    end
    while @htmltext like '%' + char(10) + ' %'
    begin
        set @htmltext = replace(@htmltext,char(10)+' ', char(10))
    end
    while @htmltext like '%' + char(10) + char(10) + '%'
    begin
        set @htmltext = replace(@htmltext,char(10)+char(10), char(10))
    end
    return ltrim(rtrim(@htmltext))
end
GO
