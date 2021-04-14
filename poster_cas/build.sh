cd src

docker build -t poster_cas_image .
docker tag poster_cas_image 127.0.0.1:5000/poster_cas_image
docker push 127.0.0.1:5000/poster_cas_image

# Cleanup commands
#kubectl get deployment |grep poster_cas | awk '{print $1}' | xargs kubectl delete deployment
#kubectl get service |grep poster_cas | awk '{print $1}' | xargs kubectl delete service
#kubectl get pods |grep poster_cas | awk '{print $1}' | xargs kubectl delete pods