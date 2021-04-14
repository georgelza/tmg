cd src

docker build -t poster_cas_image .
docker tag poster_cas_image 127.0.0.1:5000/poster_cas_image
docker push 127.0.0.1:5000/poster_cas_image


kubectl get pods |grep payee_con| awk '{print $1}' | xargs kubectl delete pods




