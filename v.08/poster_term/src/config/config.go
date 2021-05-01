package config

// Configurations exported
type Configurations struct {
	GRPC_Server ServerConfigurations
	DEBUGLEVEL  string
	Database    DatabaseConfigurations
}

// ServerConfigurations exported
type ServerConfigurations struct {
	Port string
	Name string
}

// DatabaseConfigurations exported
type DatabaseConfigurations struct {
	DBName     string
	DBUser     string
	DBPassword string
}
