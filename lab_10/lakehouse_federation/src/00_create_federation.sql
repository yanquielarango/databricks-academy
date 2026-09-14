EXECUTE IMMEDIATE
    'CREATE CONNECTION IF NOT EXISTS `' || REPLACE(:connection_name, '`', '``') || '` ' ||
    'TYPE POSTGRESQL OPTIONS (' ||
        'host ''' || REPLACE(:neon_host, '''', '''''') || ''', ' ||
        'port ''5432'', ' ||
        'user secret(''neon'', ''username''), ' ||
        'password secret(''neon'', ''password'')' ||
    ')';

EXECUTE IMMEDIATE
    'CREATE FOREIGN CATALOG IF NOT EXISTS `' || REPLACE(:foreign_catalog, '`', '``') || '` ' ||
    'USING CONNECTION `' || REPLACE(:connection_name, '`', '``') || '` ' ||
    'OPTIONS (' ||
        'database ''dvdrental''' ||
    ')';