USE Customers;
GO

-- Comprehensive query to show logins, users, roles, and read permissions
SELECT DISTINCT
    -- Login Information
    sp.name AS LoginName,
    sp.type_desc AS LoginType,
    sp.create_date AS LoginCreateDate,
    sp.is_disabled AS IsLoginDisabled,
    
    -- Database User Information
    dp.name AS DatabaseUserName,
    dp.type_desc AS UserType,
    dp.create_date AS UserCreateDate,
    
    -- Role Membership
    roles.name AS RoleName,
    
    -- Schema Information
    schemas.name AS SchemaName,
    
    -- Object-level Permissions
    perm.class_desc AS PermissionClass,
    perm.permission_name AS PermissionName,
    perm.state_desc AS PermissionState,
    
    -- Object Details (for object-level permissions)
    OBJECT_SCHEMA_NAME(perm.major_id) AS ObjectSchema,
    OBJECT_NAME(perm.major_id) AS ObjectName,
    obj.type_desc AS ObjectType

FROM sys.database_principals dp

    -- Link to server principals (logins)
    LEFT JOIN sys.server_principals sp 
        ON dp.sid = sp.sid
    
    -- Get role memberships
    LEFT JOIN sys.database_role_members drm 
        ON dp.principal_id = drm.member_principal_id
    LEFT JOIN sys.database_principals roles 
        ON drm.role_principal_id = roles.principal_id
    
    -- Get schema ownership
    LEFT JOIN sys.schemas schemas 
        ON dp.principal_id = schemas.principal_id
    
    -- Get explicit permissions
    LEFT JOIN sys.database_permissions perm 
        ON dp.principal_id = perm.grantee_principal_id
    
    -- Get object details for object-level permissions
    LEFT JOIN sys.objects obj 
        ON perm.major_id = obj.object_id

WHERE 
    -- Exclude system users
    dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys', 'MS_DataCollectorInternalUser')
    AND dp.type IN ('S', 'U', 'G')  -- SQL User, Windows User, Windows Group
    
    -- Filter for READ permissions
    AND (
        -- Database role with read access
        roles.name IN ('db_datareader', 'db_owner')
        
        -- Or explicit SELECT permission
        OR perm.permission_name IN ('SELECT', 'VIEW DEFINITION', 'CONTROL')
        
        -- Or schema owner (has implicit permissions)
        OR schemas.name IS NOT NULL
    )

ORDER BY 
    LoginName, 
    DatabaseUserName, 
    RoleName, 
    SchemaName, 
    ObjectName;

USE Customers;
GO

-- Simplified view of read access
SELECT 
    sp.name AS LoginName,
    dp.name AS DatabaseUserName,
    STRING_AGG(DISTINCT roles.name, ', ') AS Roles,
    STRING_AGG(DISTINCT schemas.name, ', ') AS OwnedSchemas,
    CASE 
        WHEN MAX(CASE WHEN roles.name IN ('db_datareader', 'db_owner') THEN 1 ELSE 0 END) = 1 
        THEN 'Yes - Via Role'
        WHEN MAX(CASE WHEN perm.permission_name = 'SELECT' THEN 1 ELSE 0 END) = 1 
        THEN 'Yes - Via Explicit Permission'
        WHEN MAX(CASE WHEN schemas.name IS NOT NULL THEN 1 ELSE 0 END) = 1 
        THEN 'Yes - Via Schema Ownership'
        ELSE 'No'
    END AS HasReadAccess

FROM sys.database_principals dp
    LEFT JOIN sys.server_principals sp ON dp.sid = sp.sid
    LEFT JOIN sys.database_role_members drm ON dp.principal_id = drm.member_principal_id
    LEFT JOIN sys.database_principals roles ON drm.role_principal_id = roles.principal_id
    LEFT JOIN sys.schemas schemas ON dp.principal_id = schemas.principal_id
    LEFT JOIN sys.database_permissions perm ON dp.principal_id = perm.grantee_principal_id 
        AND perm.permission_name IN ('SELECT', 'CONTROL')

WHERE 
    dp.type IN ('S', 'U', 'G')
    AND dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')

GROUP BY 
    sp.name, 
    dp.name

HAVING 
    MAX(CASE WHEN roles.name IN ('db_datareader', 'db_owner') THEN 1 ELSE 0 END) = 1
    OR MAX(CASE WHEN perm.permission_name = 'SELECT' THEN 1 ELSE 0 END) = 1
    OR MAX(CASE WHEN schemas.name IS NOT NULL THEN 1 ELSE 0 END) = 1

ORDER BY 
    LoginName, 
    DatabaseUserName;


