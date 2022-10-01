package transform

import (
	"github.com/Edilberto-Vazquez/weather-services/src/models"
)

func TransformEFMLines(logEventsMap models.EFMLogEvents, electricFields *models.EFMElectricFields) (processedLines *models.EFMTransformedLines) {
	processedLines = &models.EFMTransformedLines{
		FileName:         electricFields.FileName,
		TransformedLines: make([]interface{}, 0),
	}
	for _, electricField := range electricFields.ElectricFields {
		transformedLine := models.EFMTransformedLine{
			DateTime:      electricField.DateTime,
			ElectricField: electricField.ElectricField,
			RotorFail:     electricField.RotorFail,
		}
		value, exist := logEventsMap[electricField.DateTime]
		if exist {
			transformedLine.Lightning = value.Lightning
			transformedLine.Distance = value.Distance
		}
		processedLines.TransformedLines = append(processedLines.TransformedLines, transformedLine)
	}
	return processedLines
}
