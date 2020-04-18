package main

type modelGames struct {
	Objects []modelGame `json:"objects"`
}


type modelGame struct {
	PointsGained int `json:"points_gained,string"`
	WinStatus    int `json:"win_status,string"`
	GameType     int `json:"game_type,string"`
	Created      string `json:"created"`
}

type modelUsers struct {
	Objects []modelUser `json:"objects"`
}

type modelUser struct {
	Email     string `json:"email" validate:"nonzero,regexp=@"`
	LastName  string `json:"last_name" validate:"nonzero,regexp=^[a-zA-Z]+$"`
	Country   string `json:"country" validate:"nonzero,regexp=^[a-zA-Z]+$"`
	City      string `json:"city" validate:"nonzero,regexp=^[a-zA-Z]+$"`
	Gender    string `json:"gender" validate:"nonzero`
	Birthdate string `json:"birth_date" validate:"nonzero"`
}