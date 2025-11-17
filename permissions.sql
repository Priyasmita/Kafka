USE Customers;
GO

WITH UserPermissions AS (
    -- Get direct user permissions and role memberships
    SELECT 
        dp.principal_id,
        dp.name AS PrincipalName,
        dp.type_desc AS PrincipalType,
        roles.name AS RoleName,
        schemas.name AS SchemaName,
        perm.permission_name AS DirectPermission,
        perm.state_desc AS PermissionState,
        grantor.name AS GrantorName,
        grantor.principal_id AS GrantorId,
        OBJECT_SCHEMA_NAME(perm.major_id) AS ObjectSchema,
        OBJECT_NAME(perm.major_id) AS ObjectName
    FROM sys.database_principals dp
        LEFT JOIN sys.database_role_members drm 
            ON dp.principal_id = drm.member_principal_id
        LEFT JOIN sys.database_principals roles 
            ON drm.role_principal_id = roles.principal_id
        LEFT JOIN sys.schemas schemas 
            ON dp.principal_id = schemas.principal_id
        LEFT JOIN sys.database_permissions perm 
            ON dp.principal_id = perm.grantee_principal_id
            AND perm.permission_name IN ('SELECT', 'CONTROL', 'VIEW DEFINITION', 'CONNECT')
        LEFT JOIN sys.database_principals grantor 
            ON perm.grantor_principal_id = grantor.principal_id
    WHERE dp.type IN ('S', 'U', 'G')
        AND dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys', 'public')
),
GrantorPermissions AS (
    -- Get grantor's permissions (roles and explicit permissions)
    SELECT 
        dp.principal_id AS GrantorId,
        dp.name AS GrantorName,
        roles.name AS GrantorRoleName,
        schemas.name AS GrantorSchemaName,
        perm.permission_name AS GrantorPermission,
        perm.state_desc AS GrantorPermissionState,
        OBJECT_SCHEMA_NAME(perm.major_id) AS GrantorObjectSchema,
        OBJECT_NAME(perm.major_id) AS GrantorObjectName
    FROM sys.database_principals dp
        LEFT JOIN sys.database_role_members drm 
            ON dp.principal_id = drm.member_principal_id
        LEFT JOIN sys.database_principals roles 
            ON drm.role_principal_id = roles.principal_id
        LEFT JOIN sys.schemas schemas 
            ON dp.principal_id = schemas.principal_id
        LEFT JOIN sys.database_permissions perm 
            ON dp.principal_id = perm.grantee_principal_id
            AND perm.permission_name IN ('SELECT', 'CONTROL', 'VIEW DEFINITION')
    WHERE dp.type IN ('S', 'U', 'G', 'R')  -- Include roles as potential grantors
        AND dp.name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys')
)
SELECT DISTINCT
    -- Login Information
    sp.name AS LoginName,
    sp.type_desc AS LoginType,
    sp.is_disabled AS IsLoginDisabled,
    
    -- Database User Information
    up.PrincipalName AS DatabaseUserName,
    up.PrincipalType AS UserType,
    
    -- User's Direct Permissions
    up.RoleName AS UserRoleName,
    up.SchemaName AS UserSchemaName,
    up.DirectPermission AS UserDirectPermission,
    up.PermissionState AS UserPermissionState,
    up.ObjectSchema AS UserObjectSchema,
    up.ObjectName AS UserObjectName,
    
    -- Grantor Information
    up.GrantorName AS GrantedByPrincipal,
    
    -- Grantor's Permissions (what allows them to grant)
    gp.GrantorRoleName AS GrantorRoleName,
    gp.GrantorSchemaName AS GrantorSchemaName,
    gp.GrantorPermission AS GrantorPermission,
    gp.GrantorPermissionState AS GrantorPermissionState,
    gp.GrantorObjectSchema AS GrantorObjectSchema,
    gp.GrantorObjectName AS GrantorObjectName,
    
    -- Summary of Read Access
    CASE 
        WHEN up.RoleName IN ('db_datareader', 'db_owner', 'db_accessadmin') THEN 'Yes - Via Role: ' + up.RoleName
        WHEN up.DirectPermission IN ('SELECT', 'CONTROL') THEN 'Yes - Via Direct Permission: ' + up.DirectPermission
        WHEN up.SchemaName IS NOT NULL THEN 'Yes - Via Schema Ownership: ' + up.SchemaName
        WHEN gp.GrantorRoleName IN ('db_datareader', 'db_owner') THEN 'Yes - Via Grantor Role: ' + gp.GrantorRoleName
        WHEN gp.GrantorPermission IN ('SELECT', 'CONTROL') THEN 'Yes - Via Grantor Permission'
        ELSE 'Limited or No Read Access'
    END AS ReadAccessSummary

FROM UserPermissions up
    -- Link to server principals (logins)
    LEFT JOIN sys.server_principals sp 
        ON up.PrincipalName = SUSER_SNAME(sp.sid) 
        OR up.PrincipalName = sp.name
    
    -- Link to grantor's permissions
    LEFT JOIN GrantorPermissions gp 
        ON up.GrantorId = gp.GrantorId

WHERE 
    -- Filter for read-related permissions
    (
        up.RoleName IN ('db_datareader', 'db_owner', 'db_accessadmin')
        OR up.DirectPermission IN ('SELECT', 'CONTROL', 'VIEW DEFINITION', 'CONNECT')
        OR up.SchemaName IS NOT NULL
        OR gp.GrantorRoleName IN ('db_datareader', 'db_owner', 'db_accessadmin')
        OR gp.GrantorPermission IN ('SELECT', 'CONTROL', 'VIEW DEFINITION')
    )

ORDER BY 
    LoginName, 
    DatabaseUserName, 
    UserRoleName,
    GrantedByPrincipal;
