#!/usr/bin/env python
import os
import subprocess32 as subprocess
import sys
import logging
import time
from slave_sync_notify import SlaveServerSyncNotify
from slave_sync_env import (
    BORG_STATE_SSH,CODE_BRANCH,SYNC_STATUS_PATH,
    CODE_PATH,STATE_PATH,POLL_INTERVAL
)

logger = logging.getLogger(__name__)
# dumb-as-rocks script to poll for mercurial updates ad infinitum
if __name__ == "__main__":
    while True:
        #sync code first
        os.chdir(CODE_PATH)
        subprocess.call(['git', 'checkout', CODE_BRANCH], timeout=3600)
        subprocess.call(['git', 'pull'], timeout=7200)

        SlaveServerSyncNotify.send_last_poll_time()
        SlaveServerSyncNotify.exec_failed_sql()

        #sync file
        os.chdir(STATE_PATH)
        pull_status_file = os.path.join(SYNC_STATUS_PATH,'bitbucket')
        last_pull_time = None
        try:
            last_pull_time = os.path.getmtime(pull_status_file)
        except:
            pass
        try:
            subprocess.check_call(['hg', 'pull', '-b', 'default', '-u', '-e', BORG_STATE_SSH], timeout=3600)
        except:
            #failed.
            #if caused by synchronizaion failed. do nothing.
            #if because state repository is on incorrect status, try to run 'hg recover'
            last_pull_time2 = None
            try:
                last_pull_time2 = os.path.getmtime(pull_status_file)
            except:
                pass
            if last_pull_time == last_pull_time2:
                #No synchronization actions are performed, state repository should be on incorrect status
                logger.info("Borg state repository is on incrrect status, try to run 'hg recover'.")
                try:
                    subprocess.check_call(['hg', 'recover'], timeout=3600)
                    logger.info("Succeed to run 'hg recover'. Try to run 'hg pull' again.")
                    subprocess.call(['hg', 'pull', '-b', 'default', '-u', '-e', BORG_STATE_SSH], timeout=3600)
                except:
                    logger.info("Failed to run 'hg recover'.")

        time.sleep(POLL_INTERVAL)

