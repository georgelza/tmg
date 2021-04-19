
kubectl get pods -n tmg |grep scrubber| awk '{print $1}' | xargs kubectl delete pods -n tmg

kubectl get all -n tmg -o wide