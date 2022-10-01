package models

type EFMLogEvent struct {
	DateTime  string
	Lightning bool
	Distance  uint8
}

type EFMElectricField struct {
	DateTime      string
	ElectricField float64
	RotorFail     bool
}

type EFMTransformedLine struct {
	DateTime      string  `bson:"dateTime"`
	Lightning     bool    `bson:"lightning"`
	ElectricField float64 `bson:"electricField"`
	Distance      uint8   `bson:"distance"`
	RotorFail     bool    `bson:"rotorFail"`
}

type EFMLogEvents map[string]EFMLogEvent

type EFMElectricFields struct {
	FileName       string
	ElectricFields []EFMElectricField
}

type EFMTransformedLines struct {
	FileName         string
	TransformedLines []interface{}
}
