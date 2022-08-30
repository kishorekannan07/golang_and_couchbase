package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/couchbase/gocb/v2"
)

type cbRepo struct {
	cluster *gocb.Cluster
	Bucket  *gocb.Bucket
}

var (
	documentIdFormat = "user_%s"
)

type User struct {
	UserId  int
	Name    string
	Address string
	Type    string
}

func main() {
	fmt.Println("were going to connect couchdb")

	//Connecting to CB Db
	cbRepo := newCouch()

	//Adding users in Db
	UpsertedData1 := UpsertUser(cbRepo.Bucket, 1, "xyz", "xyz")
	UpsertedData2 := UpsertUser(cbRepo.Bucket, 2, "xyz", "xyz")
	UpsertedData3 := UpsertUser(cbRepo.Bucket, 3, "xyz", "xyz")

	fmt.Printf("UpsertedData1: %v\n", UpsertedData1)
	fmt.Printf("UpsertedData2: %v\n", UpsertedData2)
	fmt.Printf("UpsertedData3: %v\n", UpsertedData3)

	//getting the User by Id from Db
	RetrivedUser := GetUser(cbRepo.Bucket, 2)

	fmt.Printf("RetrivedUser: %v\n", RetrivedUser)

	//Deleting the user from The DB
	DeletedUser := DeleteUser(cbRepo.Bucket, 1)

	fmt.Printf("DeletedUser: %v\n", DeletedUser)

	// PostedUser := PostUser(cbRepo.Bucket, 4, "xyz", "xyz")

	// fmt.Printf("PostedUser: %v\n", PostedUser)

	//getting the list of users from the Db
	usersList := ListUsers(cbRepo)

	fmt.Printf("usersList: %v\n", usersList)

	//getting the count of records
	usersCount := UsersCount(cbRepo)

	fmt.Printf("usersCount: %v\n", usersCount)

}

// newcouch is a func which returns new cbRepo struct holds cluster and user Bucket
func newCouch() *cbRepo {
	options := gocb.ClusterOptions{
		Username: "YOUR_COUCHBASE_USERNAME",
		Password: "YOUR_COUCHBASE_PASSWORD",
	}
	cluster, err := gocb.Connect("localhost", options)

	checkErr("connection Err:", err)

	fmt.Println("connected to couchbase..")

	bucket := cluster.Bucket("user")

	return &cbRepo{
		cluster: cluster,
		Bucket:  bucket,
	}
}

//UpsertUser is a func which used to upsert the user record in CB DB
func UpsertUser(userBucket *gocb.Bucket, userId int, name string, address string) User {
	var user User
	user.UserId = userId
	user.Name = name
	user.Address = address
	user.Type = "user"

	documentId := fmt.Sprintf(documentIdFormat, strconv.Itoa(user.UserId))

	_, err := userBucket.DefaultCollection().Upsert(documentId, user, &gocb.UpsertOptions{})

	checkErr("Upsert ERR: ", err)

	result, err := userBucket.DefaultCollection().Get(documentId, &gocb.GetOptions{})

	checkErr("getdoc err:", err)

	err = result.Content(&user)

	checkErr("parse Doc err:", err)

	return user
}

//GetUser is a func which retrive the user Record for a given userId in CB DB
func GetUser(userBucket *gocb.Bucket, userId int) User {
	var user User
	user.UserId = userId
	documentId := fmt.Sprintf(documentIdFormat, strconv.Itoa(user.UserId))
	result, err := userBucket.DefaultCollection().Get(documentId, &gocb.GetOptions{})

	checkErr("getdoc err:", err)

	err = result.Content(&user)

	checkErr("parse Doc err:", err)

	return user
}

//DeleteUser is a func which Deletes the users Record for a given userId in CB DB
func DeleteUser(userBucket *gocb.Bucket, userId int) string {
	Id := strconv.Itoa(userId)
	documentId := fmt.Sprintf(documentIdFormat, Id)

	_, err := userBucket.DefaultCollection().Remove(documentId, &gocb.RemoveOptions{})

	checkErr("Delete Error: ", err)

	return "user has been deleted for userId: " + Id
}

//PostUser is a func which inserts given user Data CB DB
func PostUser(userBucket *gocb.Bucket, userId int, name string, address string) User {
	var user User
	user.UserId = userId
	user.Address = address
	user.Name = name
	user.Type = "user"
	Id := strconv.Itoa(userId)
	documentId := fmt.Sprintf(documentIdFormat, Id)
	_, err := userBucket.DefaultCollection().Insert(documentId, user, nil)

	checkErr("Delete Error: ", err)

	return user
}

//ListUsers is a func which retrives all the user record present in CB DB
func ListUsers(cbcluster *cbRepo) []map[string]interface{} {
	var user map[string]interface{}
	var users []map[string]interface{}
	querystr := "SELECT * from `user` where " + "Type='user'"
	result, err := cbcluster.cluster.Query(querystr, nil)

	checkErr("Error while retriving user Data", err)

	for result.Next() {
		err = result.Row(&user)
		checkErr("", err)
		resultMap := user["user"].(map[string]interface{})
		users = append(users, resultMap)
	}
	return users

}

//UserCount is a func which retrives Count of user record present in CB DB
func UsersCount(cbcluster *cbRepo) interface{} {
	querystr := "SELECT	count(*) FROM `user`"
	result, err := cbcluster.cluster.Query(querystr, nil)

	checkErr("Error while retriving user Data", err)

	var count map[string]interface{}

	for result.Next() {
		err = result.Row(&count)

		checkErr("check countErr", err)
	}
	// fmt.Println(count["$1"])
	return count["$1"]

}

//checkErr is a func which is used to check the err logs
func checkErr(s string, err error) {
	if err != nil {
		log.Fatal(s, err)
	}
}
