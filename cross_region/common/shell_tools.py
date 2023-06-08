import os


def run_script(script):
    print("run shell script: " + script)
    output = os.popen(script).readlines()
    print("shell output: ")
    print(output)
    job_id = output[-1].strip().split()[-1]
    return job_id
