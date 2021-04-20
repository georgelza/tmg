
#
export GRPC_SERVER_NAME=localhost
export GRPC_SERVER_PORT=9010

export DATABASE_USERNAME=george
export DATABASE_PASSWORD=postgres
export DATABASE_HOST=localhost
export DATABASE_PORT=5432
export DATABASE_NAME=json

export DEBUGLEVEL=2

go run -v server.go
