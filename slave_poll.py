#!/usr/bin/env python
import os
import subprocess32 as subprocess
import sys
import time
from slave_sync_notify import SlaveServerSyncNotify
from slave_sync_env import (
    BORG_SSH,CODE_BRANCH,
    CODE_PATH,STATE_PATH
)

# dumb-as-rocks script to poll for mercurial updates ad infinitum
if __name__ == "__main__":
    interval = int(sys.argv[1]) if (len(sys.argv) > 1) else 60
    while True:
        #sync code first
        os.chdir(CODE_PATH)
        subprocess.call(['git', 'checkout', CODE_BRANCH], timeout=3600)
        subprocess.call(['git', 'pull'], timeout=7200)

        SlaveServerSyncNotify.send_last_poll_time()
        SlaveServerSyncNotify.exec_failed_sql()

        #sync file
        os.chdir(STATE_PATH)
        subprocess.call(['hg', 'pull', '-b', 'default', '-u', '-e', BORG_SSH], timeout=3600)
        time.sleep(interval)

