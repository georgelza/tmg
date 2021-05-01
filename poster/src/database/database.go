/*
*
*
*	Postgres
*	https://www.robinwieruch.de/postgres-sql-macos-setup
*	https://www.calhoun.io/connecting-to-a-postgresql-database-with-gos-database-sql-package/
*
*	Redis
*	https://golangbyexample.com/golang-redis-client-example/
*
 */

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	glog "google.golang.org/grpc/grpclog"

	_ "github.com/lib/pq"

	"github.com/go-redis/redis"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	client  = &Redis_DBConnection{}
	grpcLog glog.LoggerV2
)

type Postgres_DBConnection struct {
	session *sql.DB
}

type Redis_DBConnection struct {
	c *redis.Client
}

type Mongo_DBConnection struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
}

func init() {

	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

}

/*
*
*	Postgres
*
 */
func Postgres_SetupDBConnection(dbname string, user string, password string, port int, host string) *Postgres_DBConnection {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	grpcLog.Infof("Postgres psqlInfo %s", psqlInfo)

	grpcLog.Infof("Postgres Compiled psqlInfo String!")

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		grpcLog.Fatalf("Error validating PostgreSQL database params : %s", fmt.Sprintf("%s: Error: %s", dbname, err))

	}
	grpcLog.Infof("Postgres Successfully Opened!")

	err = db.Ping()
	if err != nil {
		grpcLog.Fatalf("Error opening connection to PostgreSQL database : %s", fmt.Sprintf("%s: Error: %s", dbname, err))

	}
	grpcLog.Infof("Postgres Successfully Pinged!")
	grpcLog.Infof("Connected to PostgreSQL Server")

	return &Postgres_DBConnection{session: db}
}

func (db *Postgres_DBConnection) ExecuteInsert(query string, values ...interface{}) error {

	if _, err := db.session.Exec(query, values...); err != nil {

		grpcLog.Fatalf("Error Executing Insert : %s", fmt.Sprintf("%s: Error: %s", query, err))

		return err
	}

	return nil
}

func (db *Postgres_DBConnection) Close() {

	db.session.Close()

}

/*
*
*	Redis
*
 */

//Initialise Redis Connection
func Redis_SetupDBConnection(host, port string, db int, username, password string) *Redis_DBConnection {

	rInfo := fmt.Sprintf("%s:%s", host, port)

	grpcLog.Infof("Redis rInfo %s", rInfo)

	grpcLog.Infof("Redis Compiled rInfo String!")

	c := redis.NewClient(&redis.Options{
		Addr:     rInfo, // "127.0.0.1:6379",
		Password: password,
		DB:       db,
	})
	grpcLog.Infof("Redis Successfully Opened!")

	if err := c.Ping().Err(); err != nil {
		grpcLog.Fatalf("Unable to connect to Redis : %s", fmt.Sprintf("Error: %s", err))

	}
	grpcLog.Infof("Redis Successfully Pinged!")

	client.c = c
	grpcLog.Infof("Connected to Redis Server")

	return client
}

//GetKey get key
func (client *Redis_DBConnection) GetRedisKey(key string, src interface{}) error {

	value, err := client.c.Get(key).Result()
	if err == redis.Nil || err != nil {
		grpcLog.Errorf("Error Getting key : %s", fmt.Sprintf("Error: %s", err))
		return err

	}

	err = json.Unmarshal([]byte(value), &src)
	if err != nil {
		grpcLog.Errorf("Error Unmarshal val : %s", fmt.Sprintf("Error: %s", err))
		return err

	}
	return nil
}

//SetKey set key
func (client *Redis_DBConnection) SetRedisKey(key string, value interface{}, expiration time.Duration) error {

	value, err := json.Marshal(value)
	if err != nil {
		grpcLog.Errorf("Error Marshal value : %s", fmt.Sprintf("Error: %s", err))

	}
	err = client.c.Set(key, value, expiration).Err()
	if err != nil {
		grpcLog.Errorf("Error setting key : %s", fmt.Sprintf("Error: %s", err))
		return err

	}
	return nil
}

func (db *Redis_DBConnection) Close() {

	db.c.Close()

}

/*
*
*	MongoDB
*
 */
//Initialise MongoDB Connection

func MongoDB_SetupDBConnection(vhost, vport, vdatabase, vcollection, vusername, vpassword, vtimeout, vuri_options string) *Mongo_DBConnection {

	// https://www.mongodb.com/blog/post/quick-start-golang--mongodb--starting-and-setup
	// https://www.digitalocean.com/community/tutorials/how-to-use-go-with-mongodb-using-the-mongodb-go-driver
	// https://pkg.go.dev/go.mongodb.org/mongo-driver@v1.5.1/mongo
	// https://www.mongodb.com/blog/post/mongodb-go-driver-tutorial

	sec, _ := time.ParseDuration(vtimeout + "s")

	mongoURI := fmt.Sprintf("mongodb://%s:%s@%s/"+vuri_options, vusername, vpassword, vhost)

	grpcLog.Infof("MongoDB URI %s", mongoURI)

	grpcLog.Infof("MongoDB Compiled mongoURI String!")

	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))
	if err != nil {
		grpcLog.Fatal("Fatal Error creating new Client : %s", fmt.Sprintf("Error: %s", err))

	}
	grpcLog.Infof("MongoDB Successfully created new Client")

	ctx, _ := context.WithTimeout(context.Background(), sec)
	err = client.Connect(ctx)
	if err != nil {
		grpcLog.Fatal("Fatal Error connecting with new Client : %s", fmt.Sprintf("Error: %s", err))

	}
	grpcLog.Infof("MongoDB Successfully connected with new Client")

	err = client.Ping(ctx, nil)
	if err != nil {
		grpcLog.Fatal("Fatal Error Ping Failed : %s", fmt.Sprintf("Error: %s", err))

	}
	grpcLog.Infof("MongoDB Successfully Pinged!")

	database := client.Database(vdatabase)
	collection := database.Collection(vcollection)

	grpcLog.Infof("Connected to MongoDB Server")

	return &Mongo_DBConnection{client: client, database: database, collection: collection}
}

func (db *Mongo_DBConnection) Disconnect() {

	db.client.Disconnect(context.TODO())

}

//
func (db *Mongo_DBConnection) MongoDBStoreDoc(value interface{}) error {

	var ctx = context.TODO()

	_, err := db.collection.InsertOne(ctx, value)
	if err != nil {
		grpcLog.Errorf("Error inserting document : %s", fmt.Sprintf("Error: %s", err))

		return err
	}
	return nil
}

/*
*
*	MariaDB
*
 */
