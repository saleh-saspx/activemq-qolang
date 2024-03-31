package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-stomp/stomp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func InsertData(client *mongo.Client, data Data) error {
	// Make Data Collect
	collection := client.Database("teews").Collection("data")

	// insert to database
	_, err := collection.InsertOne(context.Background(), data)
	if err != nil {
		return err
	}

	return nil
}
func main() {

	// ActiveMQ Configurtion
	broker := "127.0.0.1:61614"
	username := "username"
	password := "password"
	queueName := "/topic/Name" // Topic Name
	// Connect ActiveMQ
	conn, err := stomp.Dial("tcp", broker,
		stomp.ConnOpt.Login(username, password))
	if err != nil {
		fmt.Println("Error while connecting to ActiveMQ:", err)
		return
	}
	defer conn.Disconnect()
	// Subscribe to the queue
	sub, err := conn.Subscribe(queueName, stomp.AckAuto)
	if err != nil {
		fmt.Println("Error while subscribing to queue:", err)
		return
	}
	defer sub.Unsubscribe()
	clientOptions := options.Client().ApplyURI("mongodb://mongo_user:mongo_password@mongo_host.cloud:31041/my-app?authSource=admin")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	// Loop to receive and process messages
	for {
		activeMs := <-sub.C
		jsonStr := activeMs.Body
		// Make JSON Data Type Object
		var data map[string]interface{}

		// check error on json object
		err := json.Unmarshal([]byte(jsonStr), &data)
		if err != nil {
			fmt.Println("خطا در پارس رشته JSON:", err)
			return
		}

		msg, ok := data["msg"].(string)
		if ok {
			_data := ParseText(msg, nil)
			err = InsertData(client, _data)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Println(_data)
			// print(_data)
		}

	}
}

// Station struct represents a station
type Station struct {
	ID   int    `json:"id"`
	Abbr string `json:"abbr"`
}

// Data struct represents parsed data
type Data struct {
	StationTime    time.Time
	Delay          int
	StationID      int
	HealthData     float64
	Temperature    float64
	MinV           float64
	BackupV        float64
	PGA            float64
	PGD            float64
	StationEventID int
	TriggerTime    time.Time
	ArrivalTime    string
	ArrivalTimeS   string
	Azimuth        float64
	EventPGA       float64
	EventPGD       float64
	WaveType       string
	Distance       float64
	Epicenter      string
	Magnitude      float64
}

// ParseText parses the provided message and returns the parsed data
func ParseText(msg string, delay *int) Data {
	msg = strings.ReplaceAll(msg, "\n", "")
	data := Data{}
	st := toDateTime(msg[22:41])
	data.StationTime = st
	if delay != nil {
		data.Delay = *delay
	} else {
		diff := data.StationTime.UTC().UnixNano()
		now := time.Now()
		data.Delay = int((now.UTC().UnixNano() - diff) / 1000000)
		fmt.Println(now.UTC(), data.StationTime.UTC(), data.Delay)
	}
	station := stationByAbbr(msg[0:3])
	if station.ID == 0 {
		fmt.Println(msg)
		panic("Station not found")
	}
	data.StationID = station.ID

	data.HealthData = parseFloat(msg[41:47])
	data.Temperature = parseFloat(msg[49:55])
	data.MinV = parseFloat(msg[54:60])
	data.BackupV = parseFloat(msg[58:64])
	data.PGA = parseFloat(msg[62:68])
	data.PGD = parseFloat(msg[69:75])

	if len(msg) >= 138 {
		data.StationEventID = parseInt(msg[75:79])
		data.TriggerTime = toDateTime(msg[79:98])
		data.ArrivalTime = msg[98:110]
		data.ArrivalTimeS = msg[108:120]
		data.Azimuth = parseFloat(msg[118:124])
		data.EventPGA = parseFloat(msg[124:130])
		data.EventPGD = parseFloat(msg[131:137])
		data.WaveType = msg[137:138]
		if len(msg) > 138 {
			data.Distance = parseFloat(msg[138:146])
			data.Epicenter = msg[146:165]
			data.Magnitude = parseFloat(msg[165:170])
		}
	}
	return data
}

func toDateTime(str string) time.Time {
	year, _ := strconv.Atoi(str[0:4])
	month, _ := strconv.Atoi(str[4:6])
	day, _ := strconv.Atoi(str[6:8])
	hour, _ := strconv.Atoi(str[9:11])
	minute, _ := strconv.Atoi(str[11:13])
	second, _ := strconv.Atoi(str[13:15])
	millisecond, _ := strconv.Atoi(str[16:19])

	loc, _ := time.LoadLocation("Local")
	println(millisecond * int(time.Millisecond))
	return time.Date(year, time.Month(month), day, hour, minute, second, millisecond*int(time.Millisecond), loc)
}

func stationByAbbr(abbr string) Station {
	stations := stations()
	for _, station := range stations {
		if station.Abbr == abbr {
			return station
		}
	}
	return Station{}
}

func stations() []Station {
	var stations []Station
	data, err := ioutil.ReadFile("_stations.json")
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(data, &stations); err != nil {
		panic(err)
	}
	return stations
}

func parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(err)
	}
	return f
}

func parseInt(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return i
}
