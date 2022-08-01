package prometheus_handler


import (
	"fmt"
	"reflect"
	"strings"
	"strconv"
	"sort"
)


var prometheusClient struct {
	HTTPoutput string
}

/*
func parsePromData(inputMap map[string]map[string]string) string {


}
*/


func parseCounter(count reflect.Value) string {
	return fmt.Sprintf("%v", count)
}

func parseHistogram(histogram reflect.Value, histogramType reflect.Type) map[string]string{
	histoMap := make(map[string]string)
	totalObservation := 0
	for i := 0; i < histogram.NumField(); i++ {
		bucketBound := histogramType.Field(i)
        	bucketValue := histogram.Field(i)
		count := int(bucketValue.Int())
		upperBound := strings.Split(bucketBound.Tag.Get("json"),",")[0]
		histoMap[upperBound] = fmt.Sprintf("%v",bucketValue)

		totalObservation += count
	}
	histoMap["+inf"] = strconv.Itoa(int(totalObservation))
	return histoMap
}


func makePromUntype(label string, value reflect.Value) string{
	strip := fmt.Sprintf("%v", value)
	output := fmt.Sprintf(`
# UNtype output
%s %s`, label, strip)
	return output
}

func makePromCounter(label string, count string) string{
	output := fmt.Sprintf(`
# HELP %s output
# TYPE %s counter
`, label, label)
	entry := fmt.Sprintf(`%s%s %s`, output,label, count)
	return entry+"\n"
}

func makePromHistogram(label string, histogram map[string]string) string {
	output := fmt.Sprintf(`
# HELP %s histogram output
# TYPE %s histogram`, label, label)

	//Sort bound
	var bounds []string
	for bound, _ := range histogram {
        	bounds = append(bounds, bound)
	}
	sort.Strings(bounds)

        for _,bound := range bounds {
                entry := fmt.Sprintf(`%s_bucket{le="%s"} %s`, label, bound, histogram[bound])
                output += "\n" + entry
        }
	entry := fmt.Sprintf("%s_count %s", label, histogram["+inf"])
	output += "\n\n" + entry + "\n"
	return output
}

func GenericPromDataParser(structure interface{}) string {
        //Split by fields
        //Identify histograms["HISTOGRAM"]
        //Identify counters["INT, FLOAT"]
        //Representing in reflect format to get struct fields
	typeExtract := reflect.TypeOf(structure)
	valueExtract := reflect.ValueOf(structure)
	var op string
        //Iterating over fields and compress their values
        for i := 0; i < typeExtract.NumField(); i++ {
		fieldType := typeExtract.Field(i)
		fieldValue := valueExtract.Field(i)

		promType := fieldType.Tag.Get("prom")
		promLabel := fieldType.Tag.Get("label")
		switch promType {
		 case "histogram" :
			 histogram := parseHistogram(fieldValue, fieldType.Type)
		 	 op += makePromHistogram(promLabel, histogram)
		 case "counter":
			 count := parseCounter(fieldValue)
			 op += makePromCounter(promLabel, count)
		 case "gauge":
			 //value := parseGauge(fieldVal)
		 case "untype":
			op += makePromUntype(promLabel, fieldValue)
		}
	}
	return op
}
