cd src

# Build image with new config - should change this to a config map in k8s
eval $(minikube -p minikube docker-env)
go mod vendor

docker build --tag=poster_all_image .
rm -rf vendor
