ps -ef |grep forward |grep poster-cas | awk '{print $2}' | xargs kill

kubectl get pods -n tmg |grep poster-cas| awk '{print $1}' | xargs kubectl delete pods -n tmg

kubectl port-forward --namespace tmg svc/poster-cas 9010 &

kubectl get all -n tmg -o wide