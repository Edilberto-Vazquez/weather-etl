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
	DateTime      string
	Lightning     bool
	ElectricField float64
	Distance      uint8
	RotorFail     bool
}

type EFMLogEvents map[string]EFMLogEvent

type EFMElectricFields []*EFMElectricField

type EFMTransformedLines []*EFMTransformedLine
