#!/bin/bash

echo "$$ Starting  Minikube..."
minikube start

echo "$$ Building Storage Bridges in bg..."
nohup minikube mount $(pwd):/mnt/week1 > mount_dags.log 2>&1 &
nohup minikube mount /mnt/c/Datasetw1:/mnt/c/Datasetw1 > mount_data.log 2>&1 &

echo "$$ Waiting for bridges to be built"
sleep 10

echo "$$ Killing off pods(dag-processor, worker) to refresh new mounts"
kubectl delete pods -l component=dag-processor -n airflow
kubectl delete pods -l component=worker -n airflow

# echo "$$ Forwarding airflow port to 8080 can be accessed at http://localhost:8080/"
# kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow

echo "Cluster READY!!!, Use commands : [ kubectl get pods -n airflow, kubectl port-forward pod/<pod in airflow namespace> 4040:4040 -n airflow ] to acccess spark UI"
