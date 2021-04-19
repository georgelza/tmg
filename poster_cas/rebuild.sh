cd src

# Build image with new config - should change this to a config map in k8s
eval $(minikube -p minikube docker-env)
go mod vendor

docker build --tag=poster_cas_image .
rm -rf vendor

ps -ef |grep forward |grep poster-cas | awk '{print $2}' | xargs kill

kubectl get pods -n tmg |grep poster-cas| awk '{print $1}' | xargs kubectl delete pods -n tmg

# gRPC access port
kubectl port-forward --namespace tmg svc/poster-cas 9010 &

kubectl get all -n tmg -o wide



