import time
import os
import flink_rest_client
from flink_rest_client.v1.client import FlinkRestClientV1
from prometheus_api_client import PrometheusConnect
import yaml


# 1. get offline running info
# 2. build dag and divide
# 3. generate yaml configuration
# 4. run the cross-region application
# 5. collect running metrics

FLINK_PATH = "/Users/zhangyang/opt/flink-1.16.1/bin/flink"
QUERY_CLASS="org.example.flink.FlinkRunner"
JAR_PATH="../target/flink-bencnmark-1.0-SNAPSHOT.jar"

remote_client: FlinkRestClientV1 = flink_rest_client.FlinkRestClient.get(host="localhost", port=8082)
local_client: FlinkRestClientV1 = flink_rest_client.FlinkRestClient.get(host="localhost", port=8081)
prom = PrometheusConnect(url="http://localhost:9090")

def run_job(source_rate = 10000, breakpoint = 0, p = 1, role = "Downstream", workload = "hotpages"):
    print("workload:{}, breakpoint:{}, role:{}, p:{}".format(workload, breakpoint, role, p))

    address = "localhost:8081"
    if role == "Downstream" or role == "downstream":
        address = "localhost:8082"

    cmds = [FLINK_PATH, "run", "-d", "-p", str(p), "-m", address, "-c", QUERY_CLASS, JAR_PATH, "--role", role,
            "--workload", workload, "--breakpoint", str(breakpoint)]

    if (source_rate > 0):
        cmds.append("--rate")
        cmds.append(str(source_rate))
        print("rate:{}".format(source_rate))

    script = " ".join(cmds)
    print(script)

    output = os.popen(script).readlines()
    print("shell output: ")
    print(output)
    job_id = output[-1].strip().split()[-1]
    return job_id


def restart_kafka():
    os.popen("cd /home/zjy/paper/flink-benchmark/exp/dockers/kafka && docker-compose down -d")
    os.popen("cd /home/zjy/paper/flink-benchmark/exp/dockers/kafka && docker-compose up -d")

def collect_metrics(source_job_id, upstream_job_id, downstream_job_id):
    query = 'sum by(task_name) (flink_taskmanager_job_task_operator_numRecordsInPerSecond{{job_id="{}"}})'.format(source_job_id)
    result = prom.custom_query(query)
    print(result)
    outputRate = 0
    for operator in result:
        if operator['metric']['task_name'].startswith("Sink"):
            outputRate = operator['value'][1]

    query = 'sum by(task_name) (flink_taskmanager_job_task_operator_numRecordsOutPerSecond{{job_id="{}"}})'.format(
        source_job_id)
    result = prom.custom_query(query)
    print(result)
    inputRate = 0
    for operator in result:
        if operator['metric']['task_name'].startswith("Source"):
            inputRate = operator['value'][1]

    query = 'sum by(job_name) (flink_taskmanager_job_task_operator_records_lag_max{job_id="7b240762e89bc813a18b44b28d11fa13"})'
    prom.custom_query(query)[0]['value'][1]
    return metrics

if __name__ == "__main__":

    with open('./settings/hotpages-20230605.yaml', 'r', encoding='utf-8') as f:
        settings = yaml.load(f.read(), Loader=yaml.FullLoader)

    workload= settings['workload']
    source_rates = settings['source_rate']
    breakpoints = settings['breakpoint']
    total = settings['total']
    local_p = settings['local_p']
    remote_p = settings['remote_p']

    remote_client.jobs.all()
    for rate in source_rates:
        for p in total:
            source_job_id = run_job(role="Source", source_rate=rate, workload=workload)
            job_id = run_job(role="Downstream", p=p, source_rate=rate, workload=workload)
            for i in range(15):
                collect_metrics(source_job_id, job_id)
                time.sleep(45)
            local_client.jobs.terminate(source_job_id)
            remote_client.jobs.terminate(job_id)
            restart_kafka()

        for breakpoint in breakpoints:
            for local_p_ in local_p:
                local_job_id = run_job(role="Upstream", p=local_p_, workload=workload)
                for remote_p_ in remote_p:
                    source_job_id = run_job(role="Source", source_rate=rate, workload=workload)
                    local_job_id = run_job(role="Upstream", p=local_p_, workload=workload)
                    remote_job_id = run_job(role="Downstream", p=remote_p_, workload=workload)
                    for i in range(15):
                        collect_metrics(source_job_id, local_job_id, remote_job_id)
                        time.sleep(45)
                    local_client.jobs.terminate(source_job_id)
                    local_client.jobs.terminate(local_job_id)
                    remote_client.jobs.terminate(remote_job_id)
                    restart_kafka()

