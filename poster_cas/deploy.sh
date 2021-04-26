cd src/

kubectl apply -f cas-config-map.yml -n tmg
kubectl apply -f deployment.yml -n tmg
