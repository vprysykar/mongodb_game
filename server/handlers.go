package main

import (
	"context"
	_ "context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/validator.v2"
	"log"
	"net/http"
	"strconv"
	"time"
)

//Returning struct
//{
//	"email": "Valerie_Gavin9167@nimogy.biz",
//	"last_name": "Gavin",
//	"country": "Kazakhstan",
//	"city": "Miami",
//	"gender": "Female",
//	"birth_date": "Monday, March 28, 8546 2:32 AM"
//}
func handlerGetUsers (w http.ResponseWriter, r *http.Request) {
	usersCollection := MongoCtx.Database(DBNAME).Collection("users")

	v := r.URL.Query()
	pageValue:=v.Get("page")
	limitValue := v.Get("limit")

	emailSearchValue :=v.Get("email")
	lastNameSearchValue :=v.Get("lname")
	countrySearchValue :=v.Get("country")
	citySearchValue :=v.Get("city")
	genderSearchValue :=v.Get("gender")

	filter := bson.D{}
	if len(citySearchValue) > 0 {
		filter = append(filter, bson.E{Key:"city",Value:citySearchValue})
	}
	if len(emailSearchValue) > 0 {
		filter = append(filter, bson.E{Key:"_id",Value:emailSearchValue})
	}
	if len(countrySearchValue) > 0 {
		filter = append(filter, bson.E{Key:"country",Value:countrySearchValue})
	}
	if len(lastNameSearchValue) > 0 {
		filter = append(filter, bson.E{Key:"lastname",Value:lastNameSearchValue})
	}
	if len(genderSearchValue) > 0 {
		filter = append(filter, bson.E{Key:"gender",Value:genderSearchValue})
	}

	mongoLimit := 20
	mongoSkip := 0

	if len(limitValue) > 0 {
		limit, err := strconv.Atoi(limitValue)
		log.Printf("Limit value is=%d",limit)
		if err != nil || limit <= 0 {
			http.Error(w, "Invalid limit value", http.StatusBadRequest)
			return
		}
		mongoLimit = limit
	}

	if len(pageValue) > 0 {
		page, err := strconv.Atoi(pageValue)
		log.Printf("Page value is=%d", page)
		if err != nil || page <= 0 {
			http.Error(w, "Invalid page value", http.StatusBadRequest)
			return
		}
		mongoSkip = page
	}
	findOptions := options.Find()
	findOptions.SetLimit(int64(mongoLimit)) 	//Limiting items result qty
	findOptions.SetSkip(int64(mongoSkip))		//read data offset(from n item)
	findOptions.SetMaxTime(time.Second * 10)


	cursor, err  := usersCollection.Find(context.TODO(), filter, findOptions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var Users []modelUser
	for cursor.Next(context.TODO()) {
		var row modelUser
		err := cursor.Decode(&row)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		Users = append(Users, row)
	}

	if err := cursor.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cursor.Close(context.TODO())
	fmt.Printf("Found multiple documents (array of pointers): %+v\n", Users)
	if Users == nil {
		w.WriteHeader(204)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp, err := json.Marshal(Users)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(rsp)
}


func handlerUsersAdd (w http.ResponseWriter, r *http.Request) {
	usersCollection := MongoCtx.Database(DBNAME).Collection("users")
	var newUser modelUser
	err := json.NewDecoder(r.Body).Decode(&newUser)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if newUser.Gender != "Male" && newUser.Gender != "Female" {
		msg := "incorrect gender value"
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	if errs := validator.Validate(newUser); errs != nil {
		http.Error(w, errs.Error(), http.StatusBadRequest)
		return
	}

	insertResult, err := usersCollection.InsertOne(context.TODO(), newUser)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Println("Inserted a single document: ", insertResult.InsertedID)
	msg := fmt.Sprintf("{\"DocumentId\":\"%v\"}",insertResult.InsertedID)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(msg))
}


func handlerGetGames (w http.ResponseWriter, r *http.Request) {
	usersGamesCollection := MongoCtx.Database(DBNAME).Collection("user_games")

	v := r.URL.Query()
	pageValue:=v.Get("page")
	limitValue := v.Get("limit")

	emailSearchValue :=v.Get("user")
	gameTypeSearchValue :=v.Get("type")
	pointsGainedSearchValue :=v.Get("pts")
	winStatusSearchValue :=v.Get("status")
	gameDateSearchValue :=v.Get("date")

	query := bson.M{}
	if len(gameTypeSearchValue) > 0 {
		query["game_type"] = gameTypeSearchValue
		//query = append(query, map[string]interface{}{"game_type": gameTypeSearchValue})
	}
	if len(emailSearchValue) > 0 {
		query["user"] = emailSearchValue
		//query = append(query, map[string]interface{}{"user":emailSearchValue})
	}
	if len(pointsGainedSearchValue) > 0 {
		query["points_gained"] = pointsGainedSearchValue
		//query = append(query, map[string]interface{}{"points_gained":pointsGainedSearchValue})
	}
	if len(winStatusSearchValue) > 0 {
		query["win_status"] = winStatusSearchValue
		//query = append(query, map[string]interface{}{"win_status":winStatusSearchValue})
	}
	if len(gameDateSearchValue) > 0 {
		incomeTime, err := time.Parse("01022006",gameDateSearchValue)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		incomeTimeUnix := incomeTime
		incomeTimeAddDay := incomeTime.AddDate(0,0,1)

		query["$and"] = []bson.M{}
		query["$and"] = append(query["$and"].([]bson.M), bson.M{"created": bson.M{"$gte":incomeTimeUnix}})
		query["$and"] = append(query["$and"].([]bson.M), bson.M{"created": bson.M{"$lt":incomeTimeAddDay}})

	}
	//fmt.Println("GetGamesHandler, MQL: ",query)

	mongoLimit := 20
	mongoSkip := 0

	if len(limitValue) > 0 {
		limit, err := strconv.Atoi(limitValue)
		log.Printf("Limit value is=%d",limit)
		if err != nil  || limit <=0 {
			http.Error(w, "Incorrect limit value", http.StatusBadRequest)
			return
		}
		mongoLimit = limit
	}

	if len(pageValue) > 0 {
		page, err := strconv.Atoi(pageValue)
		log.Printf("Page value is=%d", page)
		if err != nil || page <= 0 {
			http.Error(w, "Incorrect page value", http.StatusBadRequest)
			return
		}
		mongoSkip = page
	}

	findOptions := options.Find()
	findOptions.SetLimit(int64(mongoLimit)) 	//Limiting items result qty
	findOptions.SetSkip(int64(mongoSkip))		//read data offset(from n item)
	findOptions.SetMaxTime(time.Second * 10)

	cursor, err  := usersGamesCollection.Find(context.TODO(), query, findOptions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var  resultSet []modelGame
	for cursor.Next(context.TODO()) {
		var singleGame modelGame
		err := cursor.Decode(&singleGame)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resultSet =  append(resultSet, singleGame)
	}

	if err := cursor.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cursor.Close(context.TODO())

	if resultSet == nil {
		w.WriteHeader(204)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp, err := json.Marshal(resultSet)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(rsp)
}

func handlerStatsGame (w http.ResponseWriter, r *http.Request)  {

	v := r.URL.Query()
	groupByValue :=v.Get("groupby")
	if groupByValue != "date" && groupByValue != "game" {
		http.Error(w, "group by filter value error", http.StatusBadRequest)
		return
	}
	queryDateFrom := v.Get("datefrom")
	if len(queryDateFrom) == 0 {
		http.Error(w, "missing required DateFrom parameter", http.StatusBadRequest)
		return
	}
	queryDateTo := v.Get("dateto")
	if len(queryDateTo) == 0 {
		http.Error(w, "missing required DateTo parameter", http.StatusBadRequest)
		return
	}

	DateFrom, err := time.Parse("2006-01-02",queryDateFrom)
	if err != nil {
		http.Error(w, "date from filter value error", http.StatusBadRequest)
		return
	}
	DateTo,err := time.Parse("2006-01-02",queryDateTo)
	if err != nil {
		http.Error(w, "date to filter value error", http.StatusBadRequest)
		return
	}

	var group_by_stage bson.D
	project_stage := bson.D{{
		"$project", bson.D{{
			"_id",0}},
	}}

	match_stage := bson.D{{"$match", bson.D{
		//{"game_type", "10"},
		{"created", bson.D{
			{"$gte", DateFrom.UTC()},
			{"$lt",DateTo.UTC()},
		}},
	}}}

	sort_stage := bson.D{{"$sort", bson.D{
		{"_id", 1},
		{"game_date", 1},
	}}}


	if groupByValue == "date" {
		group_by_stage = bson.D{{"$group", bson.D{
			{"_id", bson.D{
				{"gtime",bson.D{
					{"$dateToString",bson.D{
						{"format","%d-%m-%Y"},
						{"date",bson.D{
							{"$toDate","$created"}},
						}},
					},
				}},
			}},
			{"games_count",bson.D{
				{"$sum",1},
			}},
			{"game_date", bson.D{
				{"$min", bson.D{
					{"$dateToString",bson.D{
						{"format","%Y-%m-%d"},
						{"date",bson.D{
							{"$toDate","$created"}},
						}},
					},
				}},
			}},
			//{"game_date", bson.D{
			//	{"$min", "$created"}},
			//
			//},
		}}}
	} else {

		group_by_stage = bson.D{{"$group", bson.D{
			{"_id", bson.D{
				{"gtype","$game_type"},
				{"gtime",bson.D{
					{"$dateToString",bson.D{
						{"format","%d-%m-%Y"},
						{"date",bson.D{
							{"$toDate","$created"}},
						}},
					},
				}},
			}},
			{"games_count",bson.D{
				{"$sum",1},
			}},
			{"games_type",bson.D{
				{"$min","$game_type"},
			}},
			{"points_earned", bson.D{
				{"$sum","$points_gained"},
			}},
			{"game_date", bson.D{
				{"$min", bson.D{
					{"$dateToString",bson.D{
						{"format","%Y-%m-%d"},
						{"date",bson.D{
							{"$toDate","$created"}},
						}},
					},
				}},
			}},
		}}}

		sort_stage = bson.D{{"$sort", bson.D{
			{"game_date", 1},
			{"games_type", 1},
		}}}
	}

	var finalPipe []bson.D
	finalPipe = append(finalPipe, match_stage)
	finalPipe = append(finalPipe, group_by_stage)
	finalPipe = append(finalPipe,project_stage)
	finalPipe = append(finalPipe, sort_stage)


	bytePipe, err := json.Marshal(finalPipe)
	if err != nil {
		log.Println("handlerStatsGame marshal error")
	}
	log.Println("handlerStatsGame:\n", string(bytePipe),"\n")



	userGamesCollection := MongoCtx.Database(DBNAME).Collection("user_games")
	opts := options.Aggregate().SetMaxTime(time.Second *40).SetAllowDiskUse(true)
	cursor, err := userGamesCollection.Aggregate(context.Background(), finalPipe, opts)
	if err != nil {
		http.Error(w, "aggregation query execution error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var rsultSet []map[string]interface{}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		doc := map[string]interface{}{}
		err := cursor.Decode(&doc)
		if err != nil{
			log.Println("Error decoding cursor value: "+ err.Error())
			continue
		}
		rsultSet = append(rsultSet, doc)
	}
	if len(rsultSet) == 0 {
		w.WriteHeader(http.StatusNoContent)
	}

	jsonByteString, err := json.Marshal(rsultSet)
	if err != nil {
		http.Error(w, "error marshaling result set: "+ err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonByteString)
}


func handlerUserRanking(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	queryPageValue:=v.Get("page")
	queryPage := 0

	if len(queryPageValue) == 0 {
		queryPage = 1
	} else{
		qqpage, err := strconv.Atoi(queryPageValue)
		if err != nil || qqpage <= 0 {
			http.Error(w, "incorrect page value", http.StatusBadRequest)
			return
		}
		queryPage = qqpage
	}

	userGamesCollection := MongoCtx.Database(DBNAME).Collection("user_games")

	group_stage :=bson.D{
		{"$group",bson.D{{
			"_id",bson.D{
				{"user","$user"}},
		},
			{"games_count", bson.D{
				{"$sum",1},
			}},
			{"gamer_email", bson.D{{
				"$min","$user"}},
			},
			{"points_earned", bson.D{
				{"$sum", "$points_gained"},
			}},
		}}}

	sort_stage := bson.D{
		{"$sort",bson.D{
			{"games_count",-1},
			{"points_earned",-1},
		}}}

	limit_stage := bson.D{{"$limit",50}}


	lookup_stage := bson.D{
		{"$lookup",bson.D{
			{"from","users"},
			{"localField", "gamer_email"},
			{"foreignField","_id"},
			{"as","gamer_info"},
		}}}

	unwind_stage := bson.D{
		{"$unwind",bson.D{
			{"path","$gamer_info"},
			{"preserveNullAndEmptyArrays", true},
		}}}

	project_stage := bson.D{{"$project",bson.D{{"gamer_info._id",0},{"_id",0}}}}

	var finalPipe []bson.D

	finalPipe = append(finalPipe, group_stage)
	finalPipe = append(finalPipe, sort_stage)

	skipValue := (queryPage-1)*50
	skip_stage:= bson.D{{"$skip",skipValue}}
	if  skipValue > 0 {
		finalPipe = append(finalPipe, skip_stage)
	}
	finalPipe = append(finalPipe, limit_stage)
	finalPipe = append(finalPipe, lookup_stage)
	finalPipe = append(finalPipe, unwind_stage)
	finalPipe = append(finalPipe, project_stage)

	opts := options.Aggregate().SetMaxTime(time.Second*40)


	bytePipe, err := json.Marshal(finalPipe)
	if err != nil {
		log.Println("handlerUserRank marshal error")
	}
	log.Println("handlerUserRank:\n", string(bytePipe),"\n")

	cursor, err := userGamesCollection.Aggregate(context.Background(), finalPipe, opts)
	if err != nil {
		http.Error(w, "aggregation query execution error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var rsultSet []map[string]interface{}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		doc := map[string]interface{}{}
		err := cursor.Decode(&doc)
		if err != nil{
			log.Println("Error decoding cursor value: "+ err.Error())
		}
		rsultSet = append(rsultSet, doc)
	}
	if len(rsultSet) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	page := queryPage-1
	for i, v := range rsultSet{
		v["rank"] = (i+1) + (page *50)
	}

	jsonByteString, err := json.Marshal(rsultSet)
	if err != nil {
		http.Error(w, "unable to marshal result value set: "+ err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Write(jsonByteString)
}