import logging
import os
import tempfile
import subprocess

from slave_sync_env import (
    GEOSERVER_PGSQL_HOST,GEOSERVER_PGSQL_PORT,GEOSERVER_PGSQL_DATABASE,GEOSERVER_PGSQL_USERNAME,
    CACHE_PATH,
    env
)
from slave_sync_task import (
    update_auth_job,update_feature_job,db_feature_task_filter,foreignkey_task_filter,remove_feature_job,update_workspace_job
)

logger = logging.getLogger(__name__)

schema_name = lambda sync_job: "{0}_{1}_{2}".format(sync_job["schema"],sync_job["data_schema"],sync_job["outdated_schema"])
table_name = lambda sync_job: "{0}:{1}".format(sync_job["schema"],sync_job["name"])

psql_cmd = ["psql","-h",GEOSERVER_PGSQL_HOST,"-p",GEOSERVER_PGSQL_PORT,"-d",GEOSERVER_PGSQL_DATABASE,"-U",GEOSERVER_PGSQL_USERNAME,"-w","-c",None]

def update_auth(sync_job,task_metadata,task_status):
    with tempfile.NamedTemporaryFile(mode="w+b", suffix=".sql") as sql_file:
        sql_file.file.write(sync_job['job_file_content'])
        sql_file.file.close()
        
        psql_cmd[len(psql_cmd) - 2] = "-f"
        psql_cmd[len(psql_cmd) - 1] = sql_file.name

        logger.info("Executing {}...".format(repr(psql_cmd)))
        psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        psql_output = psql.communicate()
        if psql_output[1] and psql_output[1].strip():
            logger.info("stderr: {}".format(psql_output[1]))
            task_status.set_message("message",psql_output[1])
        if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
            raise Exception("{0}:{1}".format(psql.returncode,task_status.get_message("message")))


def create_postgis_extension(sync_job,task_metadata,task_status):
    psql_cmd[len(psql_cmd) - 2] = "-c"
    psql_cmd[len(psql_cmd) - 1] = "CREATE EXTENSION IF NOT EXISTS postgis;"
    
    psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    psql_output = psql.communicate()
    if psql_output[1] and psql_output[1].strip():
        logger.info("stderr: {}".format(psql_output[1]))
        task_status.set_message("message",psql_output[1])

    if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
        raise Exception("{0}:{1}".format(psql.returncode,task_status.get_message("message")))

CREATE_SSO_ROLE = """DO
$$BEGIN
IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = '{2}')  THEN
    CREATE ROLE "{2}" WITH NOINHERIT LOGIN; 
    GRANT "{2}" TO "{3}";
END IF;
GRANT USAGE ON SCHEMA "{1}" TO "{2}" ;
GRANT SELECT ON ALL TABLES IN SCHEMA "{1}" TO "{2}";
ALTER DEFAULT PRIVILEGES IN SCHEMA "{1}" GRANT SELECT ON TABLES TO "{2}";

IF '{1}' != 'public' AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{1}' )THEN
    ALTER DEFAULT PRIVILEGES IN SCHEMA "{1}" REVOKE SELECT ON TABLES FROM "{1}";
    REVOKE ALL ON DATABASE "{0}"FROM "{1}";
    REVOKE ALL ON SCHEMA "{1}" FROM "{1}";
    REVOKE ALL ON ALL TABLES IN SCHEMA "{1}" FROM "{1}";
    DROP OWNED BY "{1}";
    DROP ROLE IF EXISTS "{1}"; 
END IF;
END$$;
"""

CREATE_RESTRICTED_ROLE = """DO
$$BEGIN
IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = '{2}')  THEN
    CREATE ROLE "{2}" WITH NOINHERIT LOGIN; 
    GRANT "{2}" TO "{3}";
END IF;
REVOKE ALL ON SCHEMA "{1}" FROM "{2}" ;
REVOKE ALL ON ALL TABLES IN SCHEMA "{1}" FROM "{2}";
ALTER DEFAULT PRIVILEGES IN SCHEMA "{1}" REVOKE SELECT ON TABLES FROM "{2}";

IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = '{1}')  THEN
    CREATE ROLE "{1}" WITH INHERIT LOGIN; 
    GRANT "{2}" TO "{1}";
    GRANT "{1}" TO "{3}";
END IF;
GRANT USAGE ON SCHEMA "{1}" TO "{1}" ;
GRANT SELECT ON ALL TABLES IN SCHEMA "{1}" TO "{1}";
ALTER DEFAULT PRIVILEGES IN SCHEMA "{1}" GRANT SELECT ON TABLES TO "{1}";

END$$;
"""
def create_schema(sync_job,task_metadata,task_status):
    #create schema
    psql_cmd[len(psql_cmd) - 2] = "-c"
    psql_cmd[len(psql_cmd) - 1] = ";".join(["CREATE SCHEMA IF NOT EXISTS \"{0}\"".format(s) for s in [sync_job["schema"],sync_job["data_schema"],sync_job["outdated_schema"]] if s])
    
    psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    psql_output = psql.communicate()
    if psql_output[1] and psql_output[1].strip():
        logger.info("stderr: {}".format(psql_output[1]))
        task_status.set_message("message",psql_output[1])

    if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
        raise Exception("Failed to create schema. {0}:{1}".format(psql.returncode,task_status.get_message("message")))

    #create or alter role
    auth_level = sync_job.get('auth_level',1)
    create_role_sql = None
    if auth_level == 2:
        create_role_sql = CREATE_RESTRICTED_ROLE
    else:
        create_role_sql = CREATE_SSO_ROLE

    if create_role_sql:
        #need authorization, create or alter a role
        psql_cmd[len(psql_cmd) - 2] = "-c"
        psql_cmd[len(psql_cmd) - 1] = create_role_sql.format(GEOSERVER_PGSQL_DATABASE,sync_job["schema"],"sso_access",GEOSERVER_PGSQL_USERNAME)

        psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        psql_output = psql.communicate()
        if psql_output[1] and psql_output[1].strip():
            logger.info("stderr: {}".format(psql_output[1]))
            task_status.set_message("message",psql_output[1])
    
        if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
            raise Exception("Failed to create role. {0}:{1}".format(psql.returncode,task_status.get_message("message")))
    

def move_outdated_table(sync_job,task_metadata,task_status):

    #move table to outdated schema
    #1.if data table does not exist, no need to move
    #2.if outdated table does not exist, move the data table to outdated schema
    #3.if view  does not exist, drop the outdated table, and move the data table to outdated schema
    #4.if view does not depend on the outdated schema, drop the outdated table and move the data table to outdated schema
    #5.if view depends on the outdated schema, drop the data table
    psql_cmd[len(psql_cmd) - 2] = "-c"
    psql_cmd[len(psql_cmd) -1] = """
DO 
$$BEGIN
    IF EXISTS (SELECT 1 FROM pg_class a join pg_namespace b on a.relnamespace = b.oid WHERE a.relname='{1}' and b.nspname='{0}') THEN
        IF EXISTS (SELECT 1 FROM pg_class a join pg_namespace b on a.relnamespace = b.oid WHERE a.relname='{1}' and b.nspname='{2}') THEN
            IF EXISTS (SELECT 1 FROM pg_class a join pg_namespace b on a.relnamespace = b.oid WHERE a.relname='{1}' and b.nspname='{3}') THEN
                IF EXISTS (SELECT 1 FROM (SELECT a1.* FROM pg_depend a1 JOIN (SELECT oid FROM pg_class WHERE relname='pg_class') a2 ON a1.refclassid = a2.oid JOIN (SELECT b1.oid FROM pg_class b1 JOIN pg_namespace b2 ON b1.relnamespace = b2.oid WHERE b1.relname='{1}' and b2.nspname='{3}') a3 ON a1.refobjid = a3.oid JOIN (SELECT oid FROM pg_class WHERE relname='pg_rewrite') a4 ON a1.classid = a4.oid) t1 JOIN (SELECT d1.* FROM pg_depend d1 JOIN (SELECT oid FROM pg_class WHERE relname='pg_class') d2 ON d1.refclassid = d2.oid JOIN (SELECT e1.oid FROM pg_class e1 JOIN pg_namespace e2 ON e1.relnamespace = e2.oid WHERE e1.relname='{1}' and e2.nspname='{2}') d3 ON d1.refobjid = d3.oid JOIN (SELECT oid FROM pg_class WHERE relname='pg_rewrite') d4 ON d1.classid = d4.oid) t2 ON t1.classid = t2.classid and t2.objid = t2.objid) THEN
                    DROP TABLE "{0}"."{1}";
                ELSE
                    DROP TABLE "{2}"."{1}";
                    ALTER TABLE "{0}"."{1}" SET SCHEMA "{2}";  
                END IF;
            ELSE
                DROP TABLE "{2}"."{1}";
                ALTER TABLE "{0}"."{1}" SET SCHEMA "{2}";  
            END IF;        
        ELSE
            ALTER TABLE "{0}"."{1}" SET SCHEMA "{2}";  
        END IF;
    END IF;
END$$;
""".format(sync_job["data_schema"],sync_job["name"],sync_job["outdated_schema"],sync_job["schema"])
    logger.info("Executing {}...".format(repr(psql_cmd)))
    psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    psql_output = psql.communicate()
    if psql_output[1] and psql_output[1].strip():
        logger.info("stderr: {}".format(psql_output[1]))
        task_status.set_message("message",psql_output[1])

    if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
        raise Exception("{0}:{1}".format(psql.returncode,task_status.get_message("message")))


restore_cmd = ["pg_restore", "-w", "-h", GEOSERVER_PGSQL_HOST, "-p" , GEOSERVER_PGSQL_PORT , "-d", GEOSERVER_PGSQL_DATABASE, "-U", GEOSERVER_PGSQL_USERNAME,"-O","-x","--no-tablespaces","-F",None,None]
def restore_table(sync_job,task_metadata,task_status):
    # load PostgreSQL dump into db with pg_restore
    if os.path.splitext(sync_job["data"]["local_file"])[1].lower() == ".db":
        restore_cmd[len(restore_cmd) - 2] = 'c'
    elif os.path.splitext(sync_job["data"]["local_file"])[1].lower() == ".tar":
        restore_cmd[len(restore_cmd) - 2] = 't'
    else:
        raise Exception("Unknown dumped file format({})".format(os.path.split(sync_job["data"]["local_file"])[1])
    restore_cmd[len(restore_cmd) - 1] = sync_job["data"]["local_file"]
    logger.info("Executing {}...".format(repr(restore_cmd)))
    restore = subprocess.Popen(restore_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    restore_output = restore.communicate()
    if restore_output[1] and restore_output[1].strip():
        logger.info("stderr: {}".format(restore_output[1]))
        task_status.set_message("message",restore_output[1])

    if restore.returncode != 0 or restore_output[1].find("ERROR") >= 0:
        raise Exception("{0}:{1}".format(restore.returncode,task_status.get_message("message")))

def restore_foreignkey(sync_job,task_metadata,task_status):
    psql_cmd[len(psql_cmd) - 2] = "-c"
    psql_cmd[len(psql_cmd) -1] = """
DO 
$$DECLARE
    foreign_key record;   
    create_index varchar(512);
BEGIN
    FOR foreign_key IN SELECT e.nspname AS schema,d.relname AS table, c.conname AS name,
                        (SELECT string_agg(a1.attname,',') 
                         FROM pg_attribute a1 JOIN pg_class b1 ON a1.attrelid = b1.oid JOIN pg_namespace c1 ON b1.relnamespace = c1.oid 
                         WHERE c1.nspname=e.nspname and b1.relname= d.relname and a1.attnum =any(c.conkey)
                        ) AS keys,
                        (SELECT string_agg(b1.attname,',') 
                         FROM pg_attribute b1 
                         WHERE b1.attrelid = a.oid and b1.attnum =any(c.confkey)
                        ) AS referenced_keys,
                        CASE c.confupdtype 
                            WHEN 'a' THEN  'NO ACTION'
                            WHEN 'r' THEN  'RESTRICT'
                            WHEN 'c' THEN  'CASCADE'
                            WHEN 'n' THEN  'SET NULL'
                            WHEN 'd' THEN  'SET DEFAULT'
                        END AS update_type, 
                        CASE c.confdeltype
                            WHEN 'a' THEN  'NO ACTION'
                            WHEN 'r' THEN  'RESTRICT'
                            WHEN 'c' THEN  'CASCADE'
                            WHEN 'n' THEN  'SET NULL'
                            WHEN 'd' THEN  'SET DEFAULT'
                        END AS del_type, 
                        CASE c.confmatchtype
                            WHEN 'f' THEN  'MATCH FULL'
                            WHEN 'p' THEN  'MATCH PARTIAL'
                            WHEN 'u' THEN  'MATCH SIMPLE'
                            ELSE ''
                        END AS match_type
                       FROM pg_class a 
                            JOIN pg_namespace b ON a.relnamespace = b.oid 
                            JOIN pg_constraint c ON c.confrelid = a.oid 
                            JOIN pg_class d ON c.conrelid = d.oid 
                            JOIN pg_namespace e ON d.relnamespace = e.oid 
                       WHERE b.nspname='{2}' AND a.relname='{1}' AND c.contype='f'
    LOOP
        EXECUTE 'ALTER TABLE "' || foreign_key.schema || '"."' || foreign_key.table || '" DROP CONSTRAINT "' || foreign_key.name || '";';
        create_index := 'ALTER TABLE "' || foreign_key.schema || '"."' || foreign_key.table || '" ADD CONSTRAINT "' || foreign_key.name 
                || '" FOREIGN KEY (' || foreign_key.keys || ') REFERENCES {0}.{1}(' || foreign_key.referenced_keys || ') ' 
                || foreign_key.match_type  || ' ON DELETE ' || foreign_key.del_type || ' ON UPDATE ' || foreign_key.update_type || ';';

        EXECUTE create_index;
        RAISE NOTICE '%',create_index;
    END LOOP;
END$$;
""".format(sync_job["data_schema"],sync_job["name"],sync_job["outdated_schema"])
    logger.info("Executing {}...".format(repr(psql_cmd)))
    psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    psql_output = psql.communicate()
    if psql_output[1] and psql_output[1].strip():
        logger.info("stderr: {}".format(psql_output[1]))
        task_status.set_message("message",psql_output[1])

    if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
        raise Exception("{0}:{1}".format(psql.returncode,task_status.get_message("message")))

def create_access_view(sync_job,task_metadata,task_status):
    #create a view to access the new layer data.
    psql_cmd[len(psql_cmd) - 2] = "-c"
    psql_cmd[len(psql_cmd) -1] = "DROP VIEW IF EXISTS \"{0}\".\"{1}\" CASCADE;CREATE VIEW \"{0}\".\"{1}\" AS SELECT * FROM \"{2}\".\"{1}\";".format(sync_job["schema"],sync_job["name"],sync_job["data_schema"])
    logger.info("Executing {}...".format(repr(psql_cmd)))
    psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    psql_output = psql.communicate()
    if psql_output[1] and psql_output[1].strip():
        logger.info("stderr: {}".format(psql_output[1]))
        task_status.set_message("message",psql_output[1])

    if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
        raise Exception("{0}:{1}".format(psql.returncode,task_status.get_message("message")))


def drop_outdated_table(sync_job,task_metadata,task_status):
    #drop the outdated table
    psql_cmd[len(psql_cmd) - 2] = "-c"
    psql_cmd[len(psql_cmd) -1] = "DROP TABLE IF EXISTS \"{0}\".\"{1}\";".format(sync_job["outdated_schema"],sync_job["name"])
    logger.info("Executing {}...".format(repr(psql_cmd)))
    psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    psql_output = psql.communicate()
    if psql_output[1] and psql_output[1].strip():
        logger.info("stderr: {}".format(psql_output[1]))
        task_status.set_message("message",psql_output[1])

    if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
        raise Exception("{0}:{1}".format(psql.returncode,task_status.get_message("message")))

def drop_table(sync_job,task_metadata,task_status):
    #drop the table
    psql_cmd[len(psql_cmd) - 2] = "-c"
    psql_cmd[len(psql_cmd) -1] = "DROP VIEW IF EXISTS \"{0}\".\"{1}\" CASCADE;DROP TABLE IF EXISTS \"{2}\".\"{1}\" CASCADE;DROP TABLE IF EXISTS \"{3}\".\"{1}\" CASCADE;".format(sync_job["schema"], sync_job["name"],sync_job["data_schema"],sync_job["outdated_schema"])
    logger.info("Executing {}...".format(repr(psql_cmd)))
    psql = subprocess.Popen(psql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    psql_output = psql.communicate()
    if psql_output[1] and psql_output[1].strip():
        logger.info("stderr: {}".format(psql_output[1]))
        task_status.set_message("message",psql_output[1])

    if psql.returncode != 0 or psql_output[1].find("ERROR") >= 0:
        raise Exception("{0}:{1}".format(psql.returncode,task_status.get_message("message")))

tasks_metadata = [
                    ("update_auth"                      , update_auth_job   , None      , "update_roles", update_auth),
                    ("create_postgis_extension"         , update_feature_job, db_feature_task_filter, "postgis_extension"   , create_postgis_extension),
                    ("create_db_schema"                 , update_feature_job , db_feature_task_filter, schema_name   , create_schema),
                    ("create_db_schema"                 , update_workspace_job,db_feature_task_filter, schema_name   , create_schema),
                    ("move_outdated_table"              , update_feature_job, db_feature_task_filter, table_name    , move_outdated_table),
                    ("restore_table"                    , update_feature_job, db_feature_task_filter, table_name    , restore_table),
                    ("restore_foreignkey"               , update_feature_job, foreignkey_task_filter, table_name    , restore_foreignkey),
                    ("create_access_view"               , update_feature_job, db_feature_task_filter, table_name    , create_access_view),
                    ("drop_outdated_table"              , update_feature_job, db_feature_task_filter, table_name    , drop_outdated_table),
                    ("drop_table"                       , remove_feature_job, db_feature_task_filter, table_name    , drop_table),
]

