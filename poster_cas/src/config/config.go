package config

// Configurations exported
type Configurations struct {
	GRPC_Server GRPC_ServerConfigurations
	Database    Cassandra_DatabaseConfigurations
	Debuglevel  string
}

// ServerConfigurations exported
type GRPC_ServerConfigurations struct {
	Port string
	Name string
}

// DatabaseConfigurations exported
type Cassandra_DatabaseConfigurations struct {
	Username   string
	Password   string
	DBHost     string
	DBPort     string
	DBKeyspace string
}
