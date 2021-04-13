package config

// Configurations exported
type Configurations struct {
	GRPC_Server GRPC_ServerConfigurations
	Database    PostgreSQL_DatabaseConfigurations
	Debuglevel  string
}

// ServerConfigurations exported
type GRPC_ServerConfigurations struct {
	Port string
	Name string
}

// DatabaseConfigurations exported
type PostgreSQL_DatabaseConfigurations struct {
	Username string
	Password string
	DBHost   string
	DBPort   int
	DBName   string
	SSLMode  string
}
