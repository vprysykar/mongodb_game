package main

import "time"

type modelUser struct {
	Email     string `json:"email" bson:"_id" validate:"nonzero,regexp=@"`
	LastName  string `json:"last_name" validate:"nonzero,regexp=^[a-zA-Z]+$"`
	Country   string `json:"country" validate:"nonzero,regexp=^[a-zA-Z]+$"`
	City      string `json:"city" validate:"nonzero,regexp=^[a-zA-Z]+$"`
	Gender    string `json:"gender" validate:"nonzero`
	Birthdate string `json:"birth_date" validate:"nonzero"`
}

type modelGame struct {
	User		 string `json:"user"`
	PointsGained string `json:"points_gained" bson:"points_gained"`
	WinStatus    string `json:"win_status" bson:"win_status"`
	GameType     string `json:"game_type" bson:"game_type"`
	Created      time.Time `json:"created"`
}




