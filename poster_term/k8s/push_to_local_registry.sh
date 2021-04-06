#PROJECT=$(gcloud config list --format 'value(core.project)')
#export REPO=gcr.io/$PROJECT

docker tag psplogger_image:$VERSION $REPO/psplogger_image:$VERSION
docker push $REPO/psplogger_image:$VERSION