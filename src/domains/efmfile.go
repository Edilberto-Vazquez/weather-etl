package domains

import (
	"time"
)

type EFMElectricField struct {
	DateTime      time.Time `bson:"date_time"`
	Lightning     bool      `bson:"lightning"`
	ElectricField float64   `bson:"electric_field"`
	Distance      uint8     `bson:"distance"`
	RotorFail     bool      `bson:"rotor_fail"`
}
