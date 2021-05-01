cd src

#ps -ef |grep forward |grep poster-postgres | awk '{print $2}' | xargs kill

kubectl apply -f postgres-config-map.yml -n tmg

kubectl get pods -n tmg |grep poster-postgres| awk '{print $1}' | xargs kubectl delete pods -n tmg

#kubectl port-forward --namespace tmg svc/poster-postgres 9010 &

kubectl get all -n tmg -o wide

cd ..