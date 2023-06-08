from prometheus_api_client import PrometheusConnect

prom = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)

query = 'flink_taskmanager_job_task_numBuffersOutPerSecond{}'
result = prom.custom_query(query)
# value = result['data']['result'][0]['value'][1]
print(result)