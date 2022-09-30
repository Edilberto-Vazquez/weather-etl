package transform

import (
	"github.com/Edilberto-Vazquez/weather-services/src/models"
)

func TransformEFMLines(logEventsMap models.EFMLogEvents, electricFields models.EFMElectricFields) (processedLines models.EFMTransformedLines) {
	for _, electricField := range electricFields {
		value, exist := logEventsMap[electricField.DateTime]
		if exist {
			processedLines = append(processedLines, &models.EFMTransformedLine{
				DateTime:      value.DateTime,
				Lightning:     value.Lightning,
				ElectricField: electricField.ElectricField,
				Distance:      value.Distance,
				RotorFail:     electricField.RotorFail,
			})
		}
	}
	return processedLines
}
