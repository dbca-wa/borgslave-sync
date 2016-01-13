Borg slave server synchronization
=================================

This project contains all the logic to receive synchronization tasks from related bitbucket repository and download data from master server or specified slave server, 
then update the latest data into database and apply the latest settings in geoserver.

The project can't be used independently, it should be used as a sub repository(code) in related borgcollector state repository and configure its hg config file (.hg/hgrc) properly.

A sample hgrc file:

    [extensions]
    strip = 

    [paths]
    default = ssh://hg@bitbucket.org/dpaw/dpaw-borgcollector-state

    [hooks]
    precommit = .hg/denied.sh
    pretxnchangegroup = code/venv/bin/honcho -e code/.env run code/venv/bin/python code/slave_sync.py

Environment variables
---------------------

This project uses Honcho as a task runner and to set environment variables (in a `.env` file). 
Required settings are as follows:

    #Specify the slave name, used for monitor; if missing, default value is host name
    SLAVE_NAME="portable_slave_rocky"

    #Server where to download the data file. 
    #if missing, all the data is downloaded from master server;
    #if specified, the slave is a portal slave and all the data is downloaded from specified server.
    SYNC_SERVER="borg@aws-borgslave-2a"
    #Data folder in server. if missing and SYNC_SERVER is specified, default value is "/opt/dpaw-borg-state/code/dumps"
    SYNC_PATH="xxx"

    #Repository branch. The code will be synchronized right before executing the synchronization task.
    CODE_BRANCH="uat"

    #ssh settings for bitbucket and rsync. if missing, default value is "ssh -i /etc/id_rsa_borg -o StrictHostKeyChecking=no -o KeepAlive=yes -o ServerAliveInterval=30 -o ConnectTimeout=3600 -o ConnectionAttempts=5"
    BORG_SSH="xxx"

    #The channels listened by the slave server, separated by ','. if missing, default value is "geoserver"
    LISTEN_CHANNELS="xxx"

    #Geoserver home url. if missing, default value is "http://localhost:8080/geoserver"
    GEOSERVER_URL="xxx"
    #Geoserver data dir. if missing, default value is "/opt/geoserver/data_dir"
    GEOSERVER_DATA_DIR="xxx"
    GEOSERVER_USERNAME="xxx"
    GEOSERVER_PASSWORD="xxx"

    #Database server. if missing, default value is "localhost"
    GEOSERVER_PGSQL_HOST="xxx"
    #Database server port. if missing, default value is "5432"
    GEOSERVER_PGSQL_PORT="xxx"
    #Database name. if missing, default value is "borg_slave"
    GEOSERVER_PGSQL_DATABASE="xxx"
    #Database schema. if missing, default value is "publish"
    GEOSERVER_PGSQL_SCHEMA="xxx"
    GEOSERVER_PGSQL_USERNAME="xxx"

    #Master database settings used to send sync status to master server. 
    #If missing, borg slave will not send sync status to master server and user can't monitor sync status from borgcollector.
    MASTER_PGSQL_HOST="xxx"
    MASTER_PGSQL_PORT="xxx"
    MASTER_PGSQL_DATABASE="xxx"
    MASTER_PGSQL_SCHEMA="xxx"
    MASTER_PGSQL_USERNAME="xxx"
    MASTER_PGSQL_PASSWORD="xxx"

    #Authentication data will not be synchronized if configred as 'true' or 'yes'
    SKIP_AUTH=true
    #Access rule will not be synchronized if configred as 'true' or 'yes'
    SKIP_RULES=true
    #Database will not be synchronized if configred as 'true' or 'yes'
    SKIP_DB=true
    #Geoserver will not be synchronized if configred as 'true' or 'yes'
    SKIP_GS=true

    #Feature filter,lambda function. 
    #if both FEATURE_FILTER and SYNC_SERVER are missing, default value is "lambda job: True"
    #if both FEATURE_FILTER is are missing, but SYNC_SERVER is not missing, default value is "lambda job:job.get('allow_authenticated',False)"
    FEATURE_FILTER="xxx"

    #Wms filter,lambda function. 
    #if both WMS_FILTER and SYNC_SERVER are missing, default value is "lambda job: True"
    #if both WMS_FILTER is are missing, but SYNC_SERVER is not missing, default value is "lambda job: False"
    WMS_FILTER="xxx"

    #Layergroup filter,lambda function. 
    #if both LAYERGROUP_FILTER and SYNC_SERVER are missing, default value is "lambda job: True"
    #if both LAYERGROUP_FILTER is are missing, but SYNC_SERVER is not missing, default value is "lambda job: False"
    LAYERGROUP_FILTER="xxx"


Running Environment Setup
--------------------------

Setup virtual environment. 

    Entry into the subrepository(code) and run:

        virtualenv venv

Install required python modules.

    Entry into the subrepository(code) and run:

        source venv/bin/activate && pip install -r requirements.txt

Synchronize manually
-----------------------

Entry into the local borgcollector state repository, run:

    hg pull

Run synchronization repeatedly
------------------------------

Entry into the local borgcollector state repository, run:

    source code/venv/bin/activate && honcho -e code/.env run python code/slave_poll.py

Run by supervisor
------------------

Put a file into /etc/supervisor/conf.d.

A sample supervisor config file "slave_poll.conf":

    [program:slave_poll]
    command=/opt/dpaw-borg-state/code/venv/bin/honcho -e /opt/dpaw-borg-state/code/.env run /opt/dpaw-borg-state/code/venv/bin/python /opt/dpaw-borg-state/code/slave_poll.py
    numprocs=1
    directory=/opt/dpaw-borg-state
    user=root
    umask=022
    priority=999
    autostart=true
    autorestart=true
    startsecs=1
    startretries=100
    exitcodes=0,2
    stopsignal=TERM
    stopwaitsecs=30
    stopasgroup=true
    redirect_stderr=false
    serverurl=AUTO
    environment=





