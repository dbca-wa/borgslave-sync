#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

/bin/bash ${SCRIPT_DIR}/init_db.sh
if [[ $? -ne 0 ]]; then
    echo "Failed to init kmi database"
    exit 1
fi

echo "Begin to initialize borg state repository"
function enable_hook() {
    if [[ "$1" == "initial_sync" ]]; then
        hook="/bin/bash ${SCRIPT_DIR}/initial_sync_in_progress.sh"
    else
        hook="python ${SCRIPT_DIR}/slave_sync.py"
    fi
    #remove the hooks
    sed -i -E '/\s*\[hooks\].*|precommit.*|pretxnchangegroup.*/d' ${BORG_STATE_HOME}/.hg/hgrc
    #add the hooks
   echo "[hooks]
precommit = /bin/bash ${SCRIPT_DIR}/denied.sh
pretxnchangegroup = ${hook}
" >> ${BORG_STATE_HOME}/.hg/hgrc
}

if [[ ! -d ${BORG_STATE_HOME}/.hg ]]; then
    echo "Borg state repository wasn't cloned before, clone it"
    hg clone -e "${BORG_STATE_SSH}" ${BORG_STATE_URL} ${BORG_STATE_HOME}
    if [[ $? -ne 0 ]]; then
        echo "Failed to clone borg state repository"
        exit 1
    fi
fi
echo "The borg state repository was cloned"

result=1
while [[ ${result} -ne 0 ]]; do
    IFS=',' read -ra URL <<< "${GEOSERVER_URL}"
    for i in "${URL[@]}"; do
        if [[ "${i}" == *"/" ]]; then
            wget ${i}web >/dev/null 2>&1
        else
            wget ${i}/web >/dev/null 2>&1
        fi
        result=$?
        if [[ ${result} -ne 0 ]]; then
            echo "The geoserver(${i}}) is not available,wait 60 seconds and check again."
            sleep 60
            break
        fi
    done
done
echo "All geoservers(${GEOSERVER_URL}) are available"

if [[ ! "$(cat ${BORG_STATE_HOME}/.hg/hgrc)" =~ "${SCRIPT_DIR}/slave_sync.py" ]]; then
    #the normal sync is not started.
    if [[ "${INITIAL_SYNC}" == "True" ]]; then
        #initial sync is required
        while true
        do
            echo "Pull the borg state repository"
            cd ${BORG_STATE_HOME} && hg pull -e "${BORG_STATE_SSH}"

            echo "Begin to perform the initial sync."
            cd ${SCRIPT_DIR} && git pull && python slave_sync.py
            if [[ $? -ne 0 ]]; then
                echo "Failed to perform the initial sync"
                sleep 60
            else
                break
            fi
        done
        echo "Succeed to perform the initial sync, enable the sync hook"
        enable_hook "sync"
    else
        echo "Enable the sync hook"
        enable_hook "sync"
    fi
fi

if [[ "$(cat ${BORG_STATE_HOME}/.hg/hgrc)" =~ "${SCRIPT_DIR}/slave_sync.py" ]]; then
    echo "The sync hook was enabled"
else
    echo "Failed to enable sync hook"
    exit 1
fi

echo "Begin to perform regular sync"

cd ${SCRIPT_DIR} && python slave_poll.py
