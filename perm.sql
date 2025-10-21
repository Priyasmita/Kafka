USE CustomersInt;
GO

-- Find principals with explicit or inherited write permissions on the LOGIN_AUDIT table
SELECT 
    perm.class_desc,
    perm.permission_name,
    perm.state_desc AS permission_state,
    prin.name AS principal_name,
    prin.type_desc AS principal_type,
    obj.name AS object_name,
    SCHEMA_NAME(obj.schema_id) AS schema_name
FROM sys.database_permissions AS perm
JOIN sys.objects AS obj 
    ON perm.major_id = obj.object_id
JOIN sys.database_principals AS prin 
    ON perm.grantee_principal_id = prin.principal_id
WHERE obj.name = 'LOGIN_AUDIT'
  AND perm.permission_name IN ('INSERT', 'UPDATE', 'DELETE', 'ALTER')
ORDER BY prin.name, perm.permission_name;

USE CustomersInt;
GO

SELECT 
    dp.name AS database_user,
    dp.type_desc AS user_type,
    sp.name AS server_login,
    sp.type_desc AS login_type,
    perm.permission_name,
    perm.state_desc AS permission_state,
    obj.name AS object_name
FROM sys.database_permissions AS perm
JOIN sys.objects AS obj
    ON perm.major_id = obj.object_id
JOIN sys.database_principals AS dp
    ON perm.grantee_principal_id = dp.principal_id
LEFT JOIN sys.server_principals AS sp
    ON dp.sid = sp.sid
WHERE obj.name = 'LOGIN_AUDIT'
  AND perm.permission_name IN ('INSERT', 'UPDATE', 'DELETE', 'ALTER')
ORDER BY dp.name, perm.permission_name;


------------------------------------------------------

SELECT 
    dp.name AS DatabaseUser,
    dp.type_desc AS UserType,
    sp.name AS LoginName,
    p.permission_name,
    p.state_desc AS PermissionState
FROM 
    sys.database_permissions p
JOIN 
    sys.objects o ON p.major_id = o.object_id
JOIN 
    sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
LEFT JOIN 
    sys.server_principals sp ON dp.sid = sp.sid
WHERE 
    o.name = 'LOGIN_AUDIT'
    AND p.permission_name IN ('INSERT', 'UPDATE', 'DELETE');


SELECT 
    r.name AS RoleName,
    m.name AS MemberName,
    sp.name AS LoginName
FROM 
    sys.database_role_members rm
JOIN 
    sys.database_principals r ON rm.role_principal_id = r.principal_id
JOIN 
    sys.database_principals m ON rm.member_principal_id = m.principal_id
LEFT JOIN 
    sys.server_principals sp ON m.sid = sp.sid
WHERE 
    r.name IN ('db_datawriter', 'custom_write_role'); -- Add any custom roles here


