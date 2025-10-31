Method 1: Simple User List

USE YourDatabaseName;
GO

SELECT 
    name AS UserName,
    type_desc AS UserType,
    create_date AS CreateDate,
    modify_date AS ModifyDate
FROM sys.database_principals
WHERE type IN ('S', 'U', 'G', 'C', 'K')
ORDER BY name;

Method 2: Users with Their Associated Logins

USE YourDatabaseName;
GO

SELECT 
    dp.name AS DatabaseUser,
    dp.type_desc AS UserType,
    sp.name AS ServerLogin,
    dp.create_date,
    CASE 
        WHEN sp.name IS NULL THEN 'Orphaned or Contained User'
        ELSE 'Has Login'
    END AS LoginStatus
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp ON dp.sid = sp.sid
WHERE dp.type IN ('S', 'U', 'G', 'C', 'K')
    AND dp.name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'dbo')
ORDER BY dp.name;

Method 3: Users with Their Roles and Permissions

USE YourDatabaseName;
GO

SELECT 
    dp.name AS UserName,
    dp.type_desc AS UserType,
    STRING_AGG(r.name, ', ') AS DatabaseRoles
FROM sys.database_principals dp
LEFT JOIN sys.database_role_members drm ON dp.principal_id = drm.member_principal_id
LEFT JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
WHERE dp.type IN ('S', 'U', 'G', 'C', 'K')
    AND dp.name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys')
GROUP BY dp.name, dp.type_desc
ORDER BY dp.name;

Method 4: Comprehensive Access Report (Detailed)

USE YourDatabaseName;
GO

SELECT 
    dp.name AS UserName,
    dp.type_desc AS UserType,
    sp.name AS LoginName,
    sp.type_desc AS LoginType,
    STRING_AGG(DISTINCT r.name, ', ') AS Roles,
    dp.authentication_type_desc AS AuthenticationType,
    dp.default_schema_name AS DefaultSchema
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp ON dp.sid = sp.sid
LEFT JOIN sys.database_role_members drm ON dp.principal_id = drm.member_principal_id
LEFT JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
WHERE dp.type IN ('S', 'U', 'G', 'C', 'K', 'E', 'X')
    AND dp.name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys')
    AND dp.principal_id > 4  -- Exclude system users
GROUP BY 
    dp.name, 
    dp.type_desc, 
    sp.name, 
    sp.type_desc,
    dp.authentication_type_desc,
    dp.default_schema_name
ORDER BY dp.name;

Method 5: Using sp_helpuser (Legacy but Simple)

USE YourDatabaseName;
GO

EXEC sp_helpuser;

USE YourDatabaseName;
GO

DECLARE @TableName NVARCHAR(128) = 'YourTableName';
DECLARE @SchemaName NVARCHAR(128) = 'dbo';

-- Direct permissions
SELECT 
    dp.name AS UserName,
    dp.type_desc AS UserType,
    'Direct' AS AccessType,
    perm.permission_name AS Permission,
    perm.state_desc AS PermissionState,
    OBJECT_SCHEMA_NAME(perm.major_id) + '.' + OBJECT_NAME(perm.major_id) AS ObjectName
FROM sys.database_principals dp
INNER JOIN sys.database_permissions perm ON dp.principal_id = perm.grantee_principal_id
WHERE perm.major_id = OBJECT_ID(@SchemaName + '.' + @TableName)
    AND dp.type IN ('S', 'U', 'G')

UNION ALL

-- Role-based permissions
SELECT 
    member.name AS UserName,
    member.type_desc AS UserType,
    'Via Role: ' + role.name AS AccessType,
    perm.permission_name AS Permission,
    perm.state_desc AS PermissionState,
    OBJECT_SCHEMA_NAME(perm.major_id) + '.' + OBJECT_NAME(perm.major_id) AS ObjectName
FROM sys.database_principals member
INNER JOIN sys.database_role_members rm ON member.principal_id = rm.member_principal_id
INNER JOIN sys.database_principals role ON rm.role_principal_id = role.principal_id
INNER JOIN sys.database_permissions perm ON role.principal_id = perm.grantee_principal_id
WHERE perm.major_id = OBJECT_ID(@SchemaName + '.' + @TableName)
    AND member.type IN ('S', 'U', 'G')

ORDER BY UserName, Permission;
