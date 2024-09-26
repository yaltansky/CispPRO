if object_id('sys_registeruser') is not null drop proc sys_registeruser
GO
create proc sys_registeruser
	@name varchar(32)
as
begin

	insert into users(id, email, PasswordHash, Salt)
	select mol_id, email, 
		'8bkjbXOpsaDUkQu0tXMPiXL8+BP/CCMHcJ7Yta1eC9M=', -- admin
		'hp1xQE0FrxtF+A6JMIh/Ig=='
	from mols
	where (
		name like @name + '%'
		or email = @name
		)
		and not exists(select 1 from users where id = mols.mol_id)

end
go
