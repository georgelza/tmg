cd src

# Build image with new config - should change this to a config map in k8s
eval $(minikube -p minikube docker-env)
go mod vendor

docker build --tag=poster_all_image .
rm -rf vendor

kubectl apply -f poster-all-config-map.yml -n tmg


kubectl get pods -n tmg |grep poster_all| awk '{print $1}' | xargs kubectl delete pods -n tmg

kubectl get all -n tmg -o wide



