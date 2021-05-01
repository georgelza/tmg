cd src

# Build image with new config - should change this to a config map in k8s
eval $(minikube -p minikube docker-env)
go mod vendor

docker build --tag=poster_postgres_image .
rm -rf vendor

kubectl apply -f postgres-config-map.yml -n tmg

#ps -ef |grep forward |grep poster-postgres | awk '{print $2}' | xargs kill

kubectl get pods -n tmg |grep poster-postgres| awk '{print $1}' | xargs kubectl delete pods -n tmg

# gRPC access port
# 
# Not used when scrubber is also running in the minikube environment
#
#kubectl port-forward --namespace tmg svc/poster-postgres 9010 &

kubectl get all -n tmg -o wide



