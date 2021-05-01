cd src

kubectl apply -f poster-all-config-map.yml -n tmg

kubectl get pods -n tmg |grep poster-all| awk '{print $1}' | xargs kubectl delete pods -n tmg

kubectl get all -n tmg -o wide

cd ..