
export GRPC_SERVER=localhost
export GRPC_PORT=9010

export KAFKA_BROKER=localhost
export KAFKA_PORT=9092
export KAFKA_TOPIC=people_pb
export KAFKA_NUMPARTITIONS=3
export KAFKA_REPLICATIONFACTOR=1
export KAFKA_RETENSION=3600000
export KAFKA_CONSUMERGROUPID=tmg1

export DEBUGLEVEL=2

go run -v scrubber.go

# https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html
# kafkacat -b localhost:9092 -t people_pb
