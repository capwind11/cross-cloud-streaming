import subprocess
import os
import argparse

FLINK_PATH = "/Users/zhangyang/opt/flink-1.16.1/bin/flink"
QUERY_CLASS="org.example.flink.FlinkRunner"
JAR_PATH="./target/flink-bencnmark-1.0-SNAPSHOT.jar"


parser = argparse.ArgumentParser()
parser.add_argument('--workload', default = 'hotpages', type=str)
parser.add_argument('--breakpoint', default = '0', type=int)
parser.add_argument('--role', default = 'Downstream', type=str)
parser.add_argument('--p', default = 1, type=int)
parser.add_argument('--rate', default = 0, type=int)

args = parser.parse_args()
workload = args.workload
breakpoint = args.breakpoint
role = args.role
p = args.p
rate = args.rate

print("workload:{}, breakpoint:{}, role:{}, p:{}".format(workload, breakpoint, role, p))

address = "localhost:8081"
if role == "Downstream" or role == "downstream":
    address = "localhost:8082"

cmds = [FLINK_PATH, "run", "-d" , "-p", str(p), "-m", address, "-c", QUERY_CLASS, JAR_PATH, "--role", role, "--workload", workload, "--breakpoint", str(breakpoint)]
if (rate > 0):
    cmds.append("--rate")
    cmds.append(str(rate))
    print("rate:{}".format(rate))

print(" ".join(cmds))

subprocess.run(cmds)
