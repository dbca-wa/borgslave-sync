import argparse
import os
import json

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
default_status_file_path = os.path.join(base_path,".sync_status")


def options():
    parser = argparse.ArgumentParser(description='List/manage the job status files',add_help=True)

    parser.add_argument('--folder', type=str, action='store',dest="folder", help='The root folder of the job status file,default is "{}"'.format(default_status_file_path),default=default_status_file_path)
    parser.add_argument('--succeed', action='store_const',const=True, dest="status",  help='Only list/manange the succeed job status files')
    parser.add_argument('--failed', action='store_const',const=False, dest="status",  help='Only list/manange the failed job status files')
    parser.add_argument('--list', action='store_const',const="list", dest="action", help='List the job status files')
    parser.add_argument('--remove', action='store_const',const="remove", dest="action", help='Remove the job status files')

    return parser.parse_args()

if __name__ == "__main__":
    args = options()
    if not args.action:
        args.action = "list"

    folders = [args.folder]
    job_status = None
    jobs = []
    maximum_len = 0
    while folders:
        for root,dirs,files in os.walk(folders.pop()):
            for f in dirs:
                folders.append(os.path.join(root,f))
            for f in files:
                if not f.lower().endswith(".json"):
                    continue
                f = os.path.join(root,f)
                with open(f) as fi:
                    job_status = json.loads(fi.read())
                if not args.status is None and args.status != job_status["status"]:
                    continue
                jobs.append((f,job_status))
                if maximum_len < len(f):
                    maximum_len = len(f)

    for f,job_status in jobs:
        if args.action == "list":
            print ("{0: <" + str(maximum_len) + "}\t\taction={1}\tstatus={2}").format(f,job_status["action"],job_status["status"])
        elif args.action == "remove":
            os.remove(f)
            print ("{0: <" + str(maximum_len) + "}\t\taction={1}\tstatus={2}\tremoved").format(f,job_status["action"],job_status["status"])
        else:
            print ("{0: <" + str(maximum_len) + "}\t\taction={1}\tstatus={2}").format(f,job_status["action"],job_status["status"])

    if args.action == "remove":
        print "{} status files are removed".format(len(jobs))
    else:
        print "{} status files are listed".format(len(jobs))
