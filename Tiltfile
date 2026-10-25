# tilt up: rebuild images on save, push to the k3d registry, deploy the Hookline chart.
# Prerequisite: make cluster-up (cluster, third-party charts and secrets).
allow_k8s_contexts('k3d-hookline')
default_registry('localhost:5050', host_from_cluster='k3d-hookline-registry:5000')

services = ['ingest', 'control', 'dispatcher', 'worker', 'scheduler', 'chaos-customer', 'hookctl']
deps = ['go.mod', 'go.sum', 'cmd', 'internal', 'gen', 'migrations', 'Dockerfile']

for s in services:
    docker_build('hookline/' + s, '.', build_args={'SERVICE': s}, only=deps)

k8s_yaml(helm(
    'deploy/helm/hookline',
    name='hookline',
    namespace='hookline',
    values=['deploy/values/local.yaml'],
    set=['image.pullPolicy=IfNotPresent'],
))

for s in services[:-1]:
    k8s_resource('hookline-' + s, labels=[s])
k8s_resource('hookline-migrate', labels=['migrate'])
k8s_resource('hookline-control', port_forwards=['8082:8080'])

local_resource('grafana', serve_cmd='kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80', labels=['monitoring'])
