import subprocess
import os

with open('docker-compose-template.yml', 'r') as f:
    content = f.read()

replacement = {
    "{{inner_port}}": "6123",
    "{{web_port}}" :"8081",
    "{{cpu}}": "0.5",
    "{{mem}}": "512m",
}

par = 4
env = "local"

for k, v in replacement.items():
    content = content.replace(k, v)
if not os.path.exists(env):
    os.mkdir(env)
with open(env+'/docker-compose.yml', 'w') as f:
    f.write(content)
os.chdir(env)
subprocess.run(["docker-compose", "up", "-d", "--scale", "taskmanager="+str(par)])