if object_id('pdms_upload') is not null drop proc pdms_upload
go
CREATE PROCEDURE [pdms_upload] (@BranchName varchar(20), @data xml) AS

-- константы
declare
  -- филиалы
  @BranchName_sez varchar(20) = 'sez'

--
create table #mfr_pdm (
  PdmDrawingNo varchar(255),
  ItemData xml
)
--
create table #mfr_pdm_items (
  PdmDrawingNo varchar(255),
  RowNo int identity(1,1),
  OptionType varchar(50),
  OptionId varchar(50),
  ItemType varchar(255),
  ParentDrwPosNo varchar(50),
  DrwPosNo varchar(50),
  DrawingNo varchar(255),
  ItemName varchar(255),
  ItemDescription varchar(255),
  UnitName varchar(50),
  QNetto float,
  UnitNameKoeff varchar(50),
  Qkoeff float,
  ExternalId varchar(255)
)

-- загрузим данные
insert into #mfr_pdm (PdmDrawingNo, ItemData)
  select
      PdmDrawingNo = x.data.value('@DrawingNo', 'varchar(255)'),
      ItemData     = x.data.query('./*')
    from (
      select DataValue = x.data.query('./*')
        from (
          select DataValue = x.data.query('./*')
            from (
              select DataValue = @data
            ) f cross apply f.DataValue.nodes('/PdmData') x(data)
        ) f cross apply f.DataValue.nodes('/Products') x(data)
    ) f cross apply f.DataValue.nodes('/PdmProductData') x(data)

-- спецификации
insert into #mfr_pdm_items (PdmDrawingNo, OptionType, OptionId, ItemType, ParentDrwPosNo, DrwPosNo, DrawingNo, ItemName, ItemDescription, UnitName, QNetto, UnitNameKoeff, Qkoeff, ExternalId)
  select
      h.PdmDrawingNo,
      --RowNo           = row_number() over (partition by h.DrawingNo order by d.data.value('@PosNo', 'varchar(255)')),
      OptionType      = d.data.value('@OptionType', 'varchar(255)'),
      OptionId        = d.data.value('@OptionId', 'varchar(255)'),
      ItemType        = d.data.value('@ItemType', 'varchar(255)'),
      ParentDrwPosNo  = d.data.value('@ParentPosNo', 'varchar(255)'),
      DrwPosNo        = d.data.value('@PosNo', 'varchar(255)'),
      DrawingNo       = d.data.value('@DrawingNo', 'varchar(255)'),
      ItemName        = d.data.value('@ItemName', 'varchar(255)'),
      ItemDescription = d.data.value('@Description', 'varchar(255)'),
      UnitName        = d.data.value('@UnitName', 'varchar(255)'),
      QNetto          = cast(replace(d.data.value('@Q', 'varchar(255)'), 'х', '') as float),
      UnitNameKoeff   = d.data.value('@UnitNameKoeff', 'varchar(255)'),
      Qkoeff          = cast(d.data.value('@Qkoeff', 'varchar(255)') as float),
      ExternalId      = d.data.value('@ExternalId', 'varchar(255)')
    from #mfr_pdm h
           cross apply h.ItemData.nodes('/Items/PdmItemData') d(data)

-- удалим нулевые нормы (фактически, всю документацию)
delete from #mfr_pdm_items where (nullif(QNetto, 0.0) is null)

-- нормализация данных
if (@BranchName = @BranchName_sez) begin
  -- исправляем номера чертежей
  update i set
      i.DrawingNo = case when (charindex('-', i.DrawingNo) = 0)
                      then replace(i.DrawingNo, '.', '')
                      else replace(left(i.DrawingNo, charindex('-', i.DrawingNo)), '.', '') +
                            right(i.DrawingNo, len(i.DrawingNo) - charindex('-', i.DrawingNo))
                    end
    from #mfr_pdm_items i
end

--
drop table
  #mfr_pdm, #mfr_pdm_items

GO
