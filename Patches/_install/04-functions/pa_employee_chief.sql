if object_id('pa_employee_chief') is not null drop function pa_employee_chief
GO
CREATE function [pa_employee_chief](@employee_id int)
  returns int
as
begin
  declare @level int = 0, @max_level int = 10, @mol_id int, @staff_position_id int, @chief_id int = null
  -- получим id физ.лица - одно из условий: подчинённый не может быть руководителем самого себя
  select @mol_id            = e.PERSON_ID,
         @staff_position_id = e.STAFF_POSITION_ID
    from dbo.PA_EMPLOYEES e with(nolock)
    where (e.EMPLOYEE_ID = @employee_id)
  -- поиск руководителя
  while (@level <= @max_level) and (@staff_position_id is not null) and ((@chief_id is null) or (@chief_id = @mol_id)) begin
    -- поднимаемся на уровень выше
    select @level             = @level + 1,
           @chief_id          = h.PERSON_ID,
           @staff_position_id = s.HEAD_POSITION_ID
      from dbo.PA_STAFF_POSITIONS s with(nolock)
             left join dbo.PA_EMPLOYEES h with(nolock) on (s.HEAD_POSITION_ID = h.STAFF_POSITION_ID) and
                                                          (isnull(h.DATE_FIRE, '20991231') >= getdate())
      where (s.STAFF_POSITION_ID = @staff_position_id)
  end
  -- возвращаем руководителя
  return @chief_id
end
GO
