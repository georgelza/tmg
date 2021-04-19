cd src

# Build image with new config - should change this to a config map in k8s
eval $(minikube -p minikube docker-env)
go mod vendor

docker build --tag=poster_postgres_image .
rm -rf vendor

ps -ef |grep forward |grep poster-postgres | awk '{print $2}' | xargs kill

kubectl get pods -n tmg |grep poster-postgres| awk '{print $1}' | xargs kubectl delete pods -n tmg

# gRPC access port
kubectl port-forward --namespace tmg svc/poster-postgres 9010 &

kubectl get all -n tmg -o wide



