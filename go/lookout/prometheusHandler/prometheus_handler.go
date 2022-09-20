package prometheus_handler

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type prometheusClient struct {
	HTTPoutput string
	Label      map[string]string
}

func parseCounter(count reflect.Value) string {
	return fmt.Sprintf("%v", count)
}

func parseHistogram(histogram reflect.Value, histogramType reflect.Type) map[string]string {
	histoMap := make(map[string]string)
	totalObservation := 0
	for i := 0; i < histogram.NumField(); i++ {
		bucketBound := histogramType.Field(i)
		bucketValue := histogram.Field(i)
		count := int(bucketValue.Int())
		upperBound := strings.Split(bucketBound.Tag.Get("json"), ",")[0]
		histoMap[upperBound] = fmt.Sprintf("%v", bucketValue)

		totalObservation += count
	}
	histoMap["+inf"] = strconv.Itoa(int(totalObservation))
	return histoMap
}

func parseGauge(gauge reflect.Value) string {
	return fmt.Sprintf("%v", gauge)
}

func makePromUntype(metric string, label string, value reflect.Value) string {
	strip := fmt.Sprintf("%v", value)
	output := fmt.Sprintf(`
# UNtype output
%s{%s} %s`, metric, label, strip)
	return output
}

func makePromCounter(metric string, label string, count string) string {
	output := fmt.Sprintf(`
# HELP %s output
# TYPE %s counter
`, metric, metric)
	entry := fmt.Sprintf(`%s%s{%s} %s`, output, metric, label, count)
	return entry + "\n"
}

func makePromHistogram(metric string, label string, histogram map[string]string) string {
	output := fmt.Sprintf(`
# HELP %s histogram output
# TYPE %s histogram`, metric, metric)

	//Sort bound
	var bounds []string
	for bound, _ := range histogram {
		bounds = append(bounds, bound)
	}
	sort.Strings(bounds)

	for _, bound := range bounds {
		entry := fmt.Sprintf(`%s_bucket{%sle="%s"} %s`, metric, label, bound, histogram[bound])
		output += "\n" + entry
	}
	entry := fmt.Sprintf("%s_count %s", metric, histogram["+inf"])
	output += "\n\n" + entry + "\n"
	return output
}

func makePromGauge(metric string, label string, value string) string {
	output := fmt.Sprintf(`
# HELP %s gauge output
# TYPE %s gauge
`, metric, metric)
	entry := fmt.Sprintf(`%s%s{%s} %s`, output, metric, label, value)
	return entry + "\n"
}

func makeLabelString(label map[string]string) string {
	output := ""
	for key, value := range label {
		output += fmt.Sprintf(`%s="%s"`, key, value)
	}
	return output
}

func GenericPromDataParser(structure interface{}, labels map[string]string) string {
	var op string

	//Reflect the struct
	typeExtract := reflect.TypeOf(structure)
	valueExtract := reflect.ValueOf(structure)
	labelString := makeLabelString(labels)

	//Iterating over fields and compress their values
	for i := 0; i < typeExtract.NumField(); i++ {
		fieldType := typeExtract.Field(i)
		fieldValue := valueExtract.Field(i)

		promType := fieldType.Tag.Get("type")
		promMetric := fieldType.Tag.Get("metric")
		switch promType {
		case "histogram":
			histogram := parseHistogram(fieldValue, fieldType.Type)
			op += makePromHistogram(promMetric, labelString, histogram)
		case "counter":
			count := parseCounter(fieldValue)
			op += makePromCounter(promMetric, labelString, count)
		case "gauge":
			value := parseGauge(fieldValue)
			op += makePromGauge(promMetric, labelString, value)
		case "untype":
			op += makePromUntype(promMetric, labelString, fieldValue)
		}
	}
	return op
}
