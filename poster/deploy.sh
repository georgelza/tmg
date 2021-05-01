cd src

kubectl apply -f poster-all-config-map.yml -n tmg

kubectl apply -f deployment.yml -n tmg

cd ..