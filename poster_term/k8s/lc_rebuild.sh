docker build -t psplogger_image .
docker tag psplogger_image localhost:5000/psplogger_image
docker push localhost:5000/psplogger_image


kubectl get pods |grep stdb-psplogger | awk '{print $1}' | xargs kubectl delete pods
kubectl get pods |grep ubnk-psplogger | awk '{print $1}' | xargs kubectl delete pods


