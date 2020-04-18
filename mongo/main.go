package main

import (
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	clsGames     modelGames
	clsUsers     modelUsers
	DBNAME = "Gamepoint1"

)

const (
	userCount = 5000
	gamesMin = 5000
	gamesMax = 6000
)



func init() {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	gamesFilePath := dir + "\\mongo\\load\\games.json"
	usersFilePath := dir + "\\mongo\\load\\users_go.json"

	gamesBytes, err := ioutil.ReadFile(gamesFilePath)
	if err != nil {
		log.Fatal(err)
	}
	usersBytes, err := ioutil.ReadFile(usersFilePath)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(gamesBytes, &clsGames)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(usersBytes, &clsUsers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(clsUsers.Objects[0])
	fmt.Println("К-во пользователей", len(clsUsers.Objects))
	fmt.Println("К-во игр", len(clsGames.Objects))


}


func main() {

	connectionString := "mongodb://localhost:27017"
	// Set client options
	clientOptions := options.Client().ApplyURI(connectionString)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}
	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")

	User_collection := client.Database(DBNAME).Collection("users")
	//user_games := client.Database(DBNAME).Collection("user_games")

	createDB := false
	testInsert := true

	simplemode := false
	channelmode :=true

	//usersCount = len(clsUsers.Objects)

	if testInsert {
		var dataToInsert []interface{}
		var operationsBulk []mongo.WriteModel
		for i:=0;i < userCount ;i++  {
			document := bson.D{{Key: "_id", Value: clsUsers.Objects[i].Email},
				{Key: "lastname", Value: clsUsers.Objects[i].LastName},
				{"gender", clsUsers.Objects[i].Gender},
				{"birthdate", clsUsers.Objects[i].Birthdate},
				{"city", clsUsers.Objects[i].City},
				{"country", clsUsers.Objects[i].Country},
			}
			dataToInsert = append(dataToInsert, document)

			singleOp := mongo.NewInsertOneModel()
			singleOp.SetDocument(document)
			operationsBulk = append(operationsBulk, singleOp)
		}
		fmt.Println(len(dataToInsert))
		fmt.Println("Operations list qty: ",len(operationsBulk))

		createUsersIndex(client)
		createGameIndex(client)


		//insertIdResult, err := User_collection.InsertMany(context.Background(), dataToInsert, options.InsertMany().SetOrdered(false))
		bulkWriteRsl, err := User_collection.BulkWrite(context.Background(), operationsBulk, options.BulkWrite().SetOrdered(false))
		if err != nil {
			log.Fatal("BulkWrite error: "+err.Error())
		}
		log.Println(bulkWriteRsl.InsertedCount)

		if simplemode {
			log.Println("Working on user_games table insertion")
			var wg sync.WaitGroup
			for i, singleUser := range clsUsers.Objects {
				wg.Add(1)
				go bulkInsertUserGames(i, singleUser.Email, client, &wg)
			}
			wg.Wait()
			fmt.Println("Goroutines done")
		}

		if channelmode {
			var input []string


			for i:=0;i < userCount ;i++  {
				input = append(input, clsUsers.Objects[i].Email)
			}


			var wg sync.WaitGroup
			// Create bufferized channel with size 5
			goroutines := make(chan string, 10)
			// Read data from input channel
			for i, userEmail := range input {
				// 1 struct{}{} - 1 goroutine
				wg.Add(1)
				//Process
				goroutines <- userEmail
				go goBulkInsertUserGames(i, userEmail, client, &wg, goroutines)
				//End process
			}
			wg.Wait()
			close(goroutines)
		}
	}


	if createDB {
		var wg sync.WaitGroup
		for i := 0;  i < len(clsUsers.Objects); i++ {
			insertResult, err := User_collection.InsertOne(context.TODO(),
				[]bson.E{{Key: "_id", Value: clsUsers.Objects[i].Email},
					{Key: "lastname", Value: clsUsers.Objects[i].LastName},
					{"gender", clsUsers.Objects[i].Gender},
					{"birthdate", clsUsers.Objects[i].Birthdate},
					{"city", clsUsers.Objects[i].City},
					{"country", clsUsers.Objects[i].Country},
				})
			if err != nil {
				log.Fatal(err)
			}
			ID := insertResult.InsertedID
			//fmt.Println("Inserted a single document: \n", ID)
			wg.Add(1)
			go bulkInsertUserGames(i, ID, client, &wg)
		}
		wg.Wait()
		fmt.Println("Goroutines done")
	}


}

func createUsersIndex(client *mongo.Client) {
	city := mongo.IndexModel{
		Keys: bson.M{"city": 1,}, Options: options.Index().SetName("city_indx")}
	gender := mongo.IndexModel{
		Keys: bson.M{"gender": 1,}, Options: options.Index().SetName("gender_indx")}
	lastname := mongo.IndexModel{
		Keys: bson.M{"lastname": 1,}, Options: options.Index().SetName("last_name_indx")}

	userIndx, err := client.Database(DBNAME).Collection("users").Indexes().CreateMany(context.TODO(), []mongo.IndexModel{city, gender, lastname}, options.CreateIndexes().SetMaxTime(time.Second*15))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("User index creation result:\n", userIndx)
}

func createGameIndex(client *mongo.Client) {
	users := mongo.IndexModel{
		Keys: bson.M{"user": 1,}, Options: options.Index().SetName("user_indx").SetBackground(true)}
	created := mongo.IndexModel{
		Keys: bson.M{"created": 1,}, Options: options.Index().SetName("created_indx").SetBackground(true)}
	gameType := mongo.IndexModel{
		Keys: bson.M{"game_type": 1,}, Options: options.Index().SetName("game_type_indx").SetBackground(true)}

	gamesIndx, err := client.Database(DBNAME).Collection("user_games").Indexes().CreateMany(context.TODO(), []mongo.IndexModel{users, created, gameType}, options.CreateIndexes().SetMaxTime(time.Second*15))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("User_games index creation result:\n", gamesIndx)
}


func bulkInsertUserGames(index int, Id interface{}, client *mongo.Client,wg *sync.WaitGroup) {
	defer wg.Done()
	userGames_collection := client.Database(DBNAME).Collection("user_games")

	rand.Seed(time.Now().UnixNano())
	min := 2000
	max := 3000
	randomNumber := min + rand.Intn(max-min+1)

	rd2min := 0
	rd2max := 600
	rnd2 := rd2min + rand.Intn(rd2max-rd2min+1)

	resultCounter := 0

	bulkWrite := true
	singleWrite := false

	if singleWrite {
		for i := 1; i <= randomNumber; i++ {
			createdTime, err := time.Parse("1/2/2006 3:04 PM", clsGames.Objects[rnd2+i].Created)
			if err != nil {
				log.Fatal(err)
				return
			}
			createdUTCTime := createdTime.UTC()

			_, err = userGames_collection.InsertOne(context.TODO(),
				[]bson.E{{Key: "user", Value: Id},
					{"created", createdUTCTime},
					{"game_type", clsGames.Objects[rnd2+i].GameType},
					{"points_gained", clsGames.Objects[rnd2+i].PointsGained},
					{"win_status", clsGames.Objects[rnd2+i].WinStatus}})

			if err != nil {
				log.Fatal(err)
			}
			resultCounter++

			//Idd := insertRow.InsertedID
			//fmt.Println("Inserted game into user_games for user: \n", Idd)
		}
		fmt.Printf("For user [%d]; email:[%v]; added [%d] games\n", index, Id, resultCounter)
	}
	if bulkWrite {
		var bulkOpts []mongo.WriteModel
		for i := 1; i <= randomNumber; i++ {
			createdTime, err := time.Parse("1/2/2006 3:04 PM", clsGames.Objects[rnd2+i].Created)
			if err != nil {
				log.Fatal(err)
				return
			}
			createdUTCTime := createdTime.UTC()

			document := bson.D{{Key: "user", Value: Id},
				{"created", createdUTCTime},
				{"game_type", clsGames.Objects[rnd2+i].GameType},
				{"points_gained", clsGames.Objects[rnd2+i].PointsGained},
				{"win_status", clsGames.Objects[rnd2+i].WinStatus}}

			singleOpt := mongo.NewInsertOneModel()
			singleOpt.SetDocument(document)

			bulkOpts = append(bulkOpts, singleOpt)

		}
		gamesBulkRsl, err := userGames_collection.BulkWrite(context.Background(), bulkOpts, options.BulkWrite().SetOrdered(false))
		if err != nil {
			log.Println("Games collection bulk write for ", Id, " error:", err.Error())
		}
		log.Println("Games collection bulk write for ", Id, " rows inserted:", gamesBulkRsl.InsertedCount)
	}

}

func goBulkInsertUserGames(index int, Id interface{}, client *mongo.Client,wg *sync.WaitGroup, goroutine chan string) {
	defer wg.Done()
	userGames_collection := client.Database(DBNAME).Collection("user_games")

	rand.Seed(time.Now().UnixNano())
	min := gamesMin
	max := gamesMax
	randomNumber := min + rand.Intn(max-min+1)

	rd2min := 0
	rd2max := 600
	rnd2 := rd2min + rand.Intn(rd2max-rd2min+1)

	resultCounter := 0

	bulkWrite := true
	singleWrite := false

	if singleWrite {
		for i := 1; i <= randomNumber; i++ {
			createdTime, err := time.Parse("1/2/2006 3:04 PM", clsGames.Objects[rnd2+i].Created)
			if err != nil {
				log.Fatal(err)
				return
			}
			createdUTCTime := createdTime.UTC()

			_, err = userGames_collection.InsertOne(context.TODO(),
				[]bson.E{{Key: "user", Value: Id},
					{"created", createdUTCTime},
					{"game_type", clsGames.Objects[rnd2+i].GameType},
					{"points_gained", clsGames.Objects[rnd2+i].PointsGained},
					{"win_status", clsGames.Objects[rnd2+i].WinStatus}})

			if err != nil {
				log.Fatal(err)
			}
			resultCounter++

			//Idd := insertRow.InsertedID
			//fmt.Println("Inserted game into user_games for user: \n", Idd)
		}
		fmt.Printf("For user [%d]; email:[%v]; added [%d] games\n", index, Id, resultCounter)
	}
	if bulkWrite {
		var bulkOpts []mongo.WriteModel
		for i := 1; i <= randomNumber; i++ {
			createdTime, err := time.Parse("1/2/2006 3:04 PM", clsGames.Objects[rnd2+i].Created)
			if err != nil {
				log.Fatal(err)
				return
			}
			createdUTCTime := createdTime.UTC()

			document := bson.D{{Key: "user", Value: Id},
				{"created", createdUTCTime},
				{"game_type", clsGames.Objects[rnd2+i].GameType},
				{"points_gained", clsGames.Objects[rnd2+i].PointsGained},
				{"win_status", clsGames.Objects[rnd2+i].WinStatus}}

			singleOpt := mongo.NewInsertOneModel()
			singleOpt.SetDocument(document)

			bulkOpts = append(bulkOpts, singleOpt)

		}
		gamesBulkRsl, err := userGames_collection.BulkWrite(context.Background(), bulkOpts, options.BulkWrite().SetOrdered(false))
		if err != nil {
			log.Println("Games collection bulk write for ", Id, " error:", err.Error())
		}
		log.Println("Games collection bulk write for ", Id, " rows inserted:", gamesBulkRsl.InsertedCount)
	}
	<-goroutine
}




