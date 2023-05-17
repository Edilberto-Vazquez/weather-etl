package domains

import (
	"time"
)

type EFMElectricField struct {
	DateTime      time.Time `bson:"datetime"`
	Lightning     bool      `bson:"lightning"`
	ElectricField float64   `bson:"electric_field"`
	Distance      uint8     `bson:"distance"`
	RotorFail     bool      `bson:"rotor_fail"`
}

type WeatherRecords struct {
	DateTime time.Time `csv:"Fecha (America/Mexico_City)" bson:"datetime"`
	TempIn   float64   `csv:"Tempin (°C)" bson:"temp_in"`
	Temp     float64   `csv:"Temp (°C)" bson:"temp"`
	Chill    float64   `csv:"Chill (°C)" bson:"chill"`
	DewIn    float64   `csv:"Dewin (°C)" bson:"dew_in"`
	Dew      float64   `csv:"Dew (°C)" bson:"dew"`
	HeatIn   float64   `csv:"Heatin (°C)" bson:"heat_in"`
	Heat     float64   `csv:"Heat (°C)" bson:"heat"`
	HumIn    float64   `csv:"Humin (%)" bson:"hum_in"`
	Hum      float64   `csv:"Hum (%)" bson:"hum"`
	WspdHi   float64   `csv:"Wspdhi (km/h)" bson:"wspd_hi"`
	WspdAvg  float64   `csv:"Wspdavg (km/h)" bson:"wspd_avg"`
	WdirAvg  float64   `csv:"Wdiravg (°)" bson:"wdir_avg"`
	Bar      float64   `csv:"Bar (mmHg)" bson:"bar"`
	Rain     float64   `csv:"Rain (mm)" bson:"rain"`
	RainRate float64   `csv:"Rainrate (mm/h)" bson:"rain_rate"`
}
