cd src

kubectl apply -f postgres-config-map.yml -n tmg

kubectl apply -f deployment.yml -n tmg

cd ..