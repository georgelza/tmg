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
	"log"
	"os"
	"time"

	glog "google.golang.org/grpc/grpclog"

	_ "github.com/lib/pq"

	"github.com/go-redis/redis"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	_ "github.com/go-sql-driver/mysql"
)

var (
	client  = &Redis_dbConnection{}
	grpcLog glog.LoggerV2
)

type Postgres_dbConnection struct {
	session *sql.DB
}

type Redis_dbConnection struct {
	c *redis.Client
}

type Mongo_dbConnection struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
}

type Maria_dbConnection struct {
	session *sql.DB
}

func init() {

	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

}

/*
*
*	Postgres
*
 */
func Postgres_dbConnect(dbname string, user string, password string, port int, host string) *Postgres_dbConnection {

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

	return &Postgres_dbConnection{session: db}
}

func (db *Postgres_dbConnection) Insert(query string, values ...interface{}) {

	if _, err := db.session.Exec(query, values...); err != nil {

		grpcLog.Fatalf("Error Executing Insert : %s", fmt.Sprintf("%s: Error: %s", query, err))
	}
}

func (db *Postgres_dbConnection) Close() {

	db.session.Close()

}

/*
*
*	Redis
*
 */

//Initialise Redis Connection
func Redis_dbConnect(host, port string, db int, username, password string) *Redis_dbConnection {

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
func (client *Redis_dbConnection) GetKey(key string, src interface{}) error {

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
func (client *Redis_dbConnection) SetKey(key string, value interface{}, expiration time.Duration) error {

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

func (db *Redis_dbConnection) Close() {

	db.c.Close()

}

/*
*
*	MongoDB
*
 */
//Initialise MongoDB Connection

func MongoDB_dbConnect(vhost, vport, vdatabase, vcollection, vusername, vpassword, vtimeout, vuri_options string) *Mongo_dbConnection {

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

	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(databases)

	database := client.Database(vdatabase)
	collection := database.Collection(vcollection)

	grpcLog.Infof("Connected to MongoDB Server")

	return &Mongo_dbConnection{client: client, database: database, collection: collection}
}

func (db *Mongo_dbConnection) Disconnect() {

	db.client.Disconnect(context.TODO())

}

//
func (db *Mongo_dbConnection) StoreDoc(value interface{}) error {

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
* 	https://www.golangprograms.com/example-of-golang-crud-using-mysql-from-scratch.html
*	https://phoenixnap.com/kb/how-to-create-mariadb-user-grant-privileges
*
 */
func MariaDB_dbConnect(dbname, user, password, port, host string) *Maria_dbConnection {

	mInfo := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)

	grpcLog.Infof("MariaDB mInfo %s", mInfo)

	grpcLog.Infof("MariaDB Compiled mInfo String!")

	db, err := sql.Open("mysql", mInfo)
	if err != nil {
		grpcLog.Fatalf("Error validating MariaDB database params : %s", fmt.Sprintf("%s: Error: %s", dbname, err))

	}
	grpcLog.Infof("MariaDB Successfully Opened!")

	err = db.Ping()
	if err != nil {
		grpcLog.Fatalf("Error opening connection to MariaDB database : %s", fmt.Sprintf("%s: Error: %s", dbname, err))

	}
	grpcLog.Infof("MariaDB Successfully Pinged!")
	grpcLog.Infof("Connected to MariaDB Server")

	return &Maria_dbConnection{session: db}
}

func (db *Maria_dbConnection) Insert(query string, values ...interface{}) {

	insForm, err := db.session.Prepare(query)
	if err != nil {
		grpcLog.Fatalf("Error Preparing Insert stmt: %s", fmt.Sprintf("%s: Error: %s", query, err.Error()))

	}

	_, err = insForm.Exec(values...)
	if err != nil {
		grpcLog.Fatalf("Error Executing Insert stmt: %s", fmt.Sprintf("%s: Error: %s", query, err))
	}

}

func (db *Maria_dbConnection) Close() {

	db.session.Close()

}
