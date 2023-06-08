from copy import deepcopy

import flink_rest_client
from flink_rest_client.v1.client import FlinkRestClientV1

from template.example import yahoo_example

class job_graph:

    def __init__(self, job_name, desc2node):
        self.job_name = job_name
        self.desc2node = desc2node
        self.edges = self.generate_graph()
        self.total_load = 0
        self.partition_schemas= []
        self.visited_marked = set()
        self.local_operators = set()

        for k, v in self.desc2node.items():
            self.total_load += v.busytime
        if self.total_load==0:
            self.total_load=1

    def search(self):
        local_operators_set = set()
        remote_operators_set = set()
        load = 0
        traffic = 0
        for k,v in self.desc2node.items():
            if (len(v.pre)==0):
                local_operators_set.add(k)
                load+=v.busytime
                traffic+=v.output
            else:
                remote_operators_set.add(k)
        self.visited_marked.add(tuple(sorted(list(local_operators_set))))
        print("local: ", local_operators_set)
        print("remote: ", remote_operators_set)
        print("traffic: ", traffic)
        print("load: ", load/self.total_load)
        print("="*50+"\n")
        self.partition_schemas.append([local_operators_set.copy(), remote_operators_set.copy(), traffic, load])
        self.search_all_schema(local_operators_set, remote_operators_set, load, traffic)
        with open("metrics/"+self.job_name+".txt", "w") as f:
            for schema in self.partition_schemas:
                f.write("local: "+ ", ".join(schema[0])+"\n")
                f.write("remote: "+", ".join(schema[1])+"\n")
                f.write("traffic: "+str(schema[2])+"\n")
                f.write("load: "+str(schema[3])+"\n")
                f.write("load ratio: "+str(schema[3]/self.total_load)+"\n")
                f.write("="*50+"\n")


    def search_all_schema(self, local_operators_set, remote_operators_set, load, traffic):

        for node in list(remote_operators_set):
            flag = True
            for pre in self.desc2node[node].pre:
                if pre not in local_operators_set:
                    flag = False
                    break
            if not flag:
                continue
            local_operators_set.add(node)
            if tuple(sorted(local_operators_set)) in self.visited_marked:
                local_operators_set.remove(node)
                continue
            remote_operators_set.remove(node)
            pre_load = load
            pre_traffic = traffic

            load += self.desc2node[node].busytime
            traffic-=self.desc2node[node].input
            traffic+=self.desc2node[node].output
            self.partition_schemas.append([local_operators_set.copy(), remote_operators_set.copy(), traffic, load])
            print("local: ", local_operators_set)
            print("remote: ", remote_operators_set)
            print("traffic: ", traffic)
            print("load: ", load/self.total_load)
            print("="*50+"\n")
            self.visited_marked.add(tuple(sorted(local_operators_set)))
            self.search_all_schema(local_operators_set, remote_operators_set, load, traffic)
            local_operators_set.remove(node)
            remote_operators_set.add(node)
            load = pre_load
            traffic = pre_traffic


    def partition(self):
        pass

    def generate_graph(self):
        edges = []
        visited = set()
        for k, v in self.desc2node.items():
            if k in visited:
                continue
            for next in v.next:
                # edges.append([k, next, desc2node[next].accumulated_load+v.output/len(v.next)])
                edges.append([k, next, v.output / len(v.next)])
            visited.add(k)
        return edges

    def generate_yaml(self):
        pass

class task_node:

    def __init__(self, id, desc="", output=0, input=0, busytime=0, next=[], pre = [], accumulated_load=0):
        self.id = id
        self.desc = desc
        self.output = output
        self.input = input
        if busytime=='NaN':
            self.busytime = 0
        else:
            self.busytime = busytime
        self.next = next
        self.pre = pre
        self.accumulated_load = accumulated_load


def extract_execution_info(job_id):

    client: FlinkRestClientV1 = flink_rest_client.FlinkRestClient.get(host="localhost", port=8082)
    # jobs = client.jobs.()
    job = client.jobs.get(job_id)
    # job = yahoo_example
    plan = job['plan']
    vertices_id = [v['id'] for v in job['vertices']]
    id2metrics = {
        v['id']: v['metrics'] for v in job['vertices']
    }
    id2next = {
        id: [] for id in vertices_id
    }

    id2pre = {
        id: [] for id in vertices_id
    }

    id2desc = {
        node['id']: node['description'] for node in plan['nodes']
    }

    for node in plan['nodes']:
        if 'inputs' in node:
            for i in node['inputs']:
                id2next[i['id']].append(id2desc[node['id']])

    for node in plan['nodes']:
        if 'inputs' in node:
            for i in node['inputs']:
                id2pre[node['id']].append(id2desc[i['id']])

    desc2node = {}

    for id in vertices_id:
        node = task_node(id, desc=id2desc[id], output=id2metrics[id]['write-bytes'],
                         input=id2metrics[id]['read-bytes'], busytime=id2metrics[id]['accumulated-busy-time'],
                         next=id2next[id], pre=id2pre[id])
        desc2node[id2desc[id]] = node

    graph = job_graph(job['name'], desc2node)
    graph.search()



# extract_execution_info("bac2fcfb61c742103f33a3902ab20fa8")
