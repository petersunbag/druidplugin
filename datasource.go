package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"golang.org/x/net/context"

	"github.com/grafana/grafana_plugin_model/go/datasource"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
)

type DruidDatasource struct {
	plugin.NetRPCUnsupportedPlugin
	logger hclog.Logger
}

//Request and Response Struct

type Result struct {
	Value float64
}

type Timestamp string

type Aggregation struct {
	Type      string `json:"type"`
	FieldName string `json:"fieldName,omitempty"`
	Name      string `json:"name"`
}

type Fields struct {
	FieldsType string `json:"type"`
	Dimension  string `json:"dimension"`
	Value      string `json:"value"`
}

type Filter struct {
	Type      string   `json:"type,omitempty"`
	Dimension string   `json:"dimension,omitempty"`
	Value     string   `json:"value,omitempty"`
	Fields    []Fields `json:"fields,omitempty"`
}

type Payload struct {
	QueryType    string        `json:"queryType"`
	DataSource   string        `json:"dataSource"`
	Granularity  string        `json:"granularity"`
	Aggregations []Aggregation `json:"aggregations"`
	Intervals    []string      `json:"intervals"`
	Filters      Filter        `json:"filter,omitempty"`
}

var aggregationName = ""

func (t *DruidDatasource) Query(ctx context.Context, tsdbReq *datasource.DatasourceRequest) (*datasource.DatasourceResponse, error) {

	t.logger.Debug("Query", "datasource", tsdbReq.Datasource.Name, "TimeRange", tsdbReq.TimeRange)
	var response *datasource.DatasourceResponse
	var err error

	modelJson, jsonerr := simplejson.NewJson([]byte(tsdbReq.Queries[0].ModelJson))
	if jsonerr != nil {
	}

	queryType := modelJson.Get("queryType").MustString()
	t.logger.Debug("Query", "Model Json type", reflect.TypeOf(modelJson))

	switch queryType {
	case "timeseries":
		response, err = t.handleTimeseries(tsdbReq)
		if err != nil {
		}
	}

	return response, nil
}

func (t *DruidDatasource) handleTimeseries(tsdbReq *datasource.DatasourceRequest) (*datasource.DatasourceResponse, error) {

	//Input data

	var FromEpochMs, error1 = strconv.ParseInt(strconv.FormatInt(tsdbReq.TimeRange.FromEpochMs, 10)[0:10], 10, 64)
	var ToEpochMs, error2 = strconv.ParseInt(strconv.FormatInt(tsdbReq.TimeRange.ToEpochMs, 10)[0:10], 10, 64)
	if error1 != nil {
	}
	if error2 != nil {
	}

	from := time.Unix(int64(FromEpochMs), 0).Format(time.RFC3339)[0:19] + "Z"
	to := time.Unix(int64(ToEpochMs), 0).Format(time.RFC3339)[0:19] + "Z"

	t.logger.Debug("Query", "Intervals", from)
	t.logger.Debug("Query", "Intervals", to)

	modelJson, jsonerr := simplejson.NewJson([]byte(tsdbReq.Queries[0].ModelJson))
	if jsonerr != nil {
	}

	t.logger.Debug("Query", "Model Json", modelJson)

	datasourceName := modelJson.Get("druidDS").MustString()
	granularity := modelJson.Get("customGranularity").MustString()
	queryType := modelJson.Get("queryType").MustString()
	aggregationName = modelJson.Get("aggregators").GetIndex(0).Get("name").MustString()
	aggregationType := modelJson.Get("aggregators").GetIndex(0).Get("type").MustString()
	aggregationFieldName := modelJson.Get("aggregators").GetIndex(0).Get("fieldName").MustString()

	filters_len := len(modelJson.Get("filters").MustArray())
	t.logger.Debug("debug", "filter len", filters_len)

	var filter Filter

	if filters_len > 1 {
		filter.Type = "and"
		filter.Fields = make([]Fields, filters_len)
		for i := range filter.Fields {
			filter.Fields[i] = Fields{
				modelJson.Get("filters").GetIndex(i).Get("type").MustString(),
				modelJson.Get("filters").GetIndex(i).Get("dimension").MustString(),
				modelJson.Get("filters").GetIndex(i).Get("value").MustString(),
			}
		}
	}
	if filters_len == 1 {
		filter.Type = modelJson.Get("filters").GetIndex(0).Get("type").MustString()
		filter.Dimension = modelJson.Get("filters").GetIndex(0).Get("dimension").MustString()
		filter.Value = modelJson.Get("filters").GetIndex(0).Get("value").MustString()
	}

	data := &Payload{
		QueryType:    queryType,
		DataSource:   datasourceName,
		Granularity:  granularity,
		Aggregations: []Aggregation{{aggregationType, aggregationFieldName, aggregationName}},
		Intervals:    []string{from + "/" + to},
		Filters:      Filter{filter.Type, filter.Dimension, filter.Value, filter.Fields},
	}

	payloadBytes, err := json.Marshal(data)
	if err != nil {
		println(err)
	}

	reqBody := string(payloadBytes)
	if filters_len == 0 {
		reg := regexp.MustCompile(`,"filter":{}`)
		reqBody = reg.ReplaceAllString(reqBody, "")
	}

	t.logger.Error("Query", "Payload", reqBody)
	url := tsdbReq.Datasource.Url + "/druid/v2"
	return t.makeRequest(reqBody, url)

}
func (t *DruidDatasource) makeRequest(reqBody string, url string) (*datasource.DatasourceResponse, error) {

	//Request
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(reqBody))

	if err != nil {
		// handle err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// handle err
		println("err in response")
	}
	t.logger.Debug("Query", "response payload", resp.Status)

	if resp.StatusCode != http.StatusOK {
		fmt.Errorf("invalid status code. status: %v", resp.Status)
	}
	return t.parseResponse(resp)
}
func (t *DruidDatasource) parseResponse(resp *http.Response) (*datasource.DatasourceResponse, error) {
	//Response

	t.logger.Debug("parseResponse", "test", aggregationName)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	t.logger.Debug("Query", "msg", body)

	//Response data parsing

	var res []map[string]interface{}
	json.Unmarshal(body, &res)

	t.logger.Debug("Query", "Response body", res)

	qr := datasource.QueryResult{
		Series: make([]*datasource.TimeSeries, 0),
	}

	serie := &datasource.TimeSeries{Name: "druid"}

	response_len := len(res)
	for i := 0; i < response_len; i++ {

		fmt.Println(i)

		timestamp, err := time.Parse(time.RFC3339, res[i]["timestamp"].(string))
		result := res[i]["result"]

		t.logger.Debug("Query", "Timestamp", timestamp)
		t.logger.Debug("Query", "Result", result)

		//timestamp,err:=strconv.ParseInt(r.Timestamp, 10, 64)
		if err != nil {
			t.logger.Error("Query", "Response points err", err)
		}
		serie.Points = append(serie.Points, &datasource.Point{
			Timestamp: timestamp.Unix(),
			Value:     result.(map[string]interface{})[aggregationName].(float64),
		})
	}

	qr.Series = append(qr.Series, serie)
	response := &datasource.DatasourceResponse{}
	response.Results = append(response.Results, &qr)

	return response, nil
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			Renegotiation: tls.RenegotiateFreelyAsClient,
		},
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
	},
	Timeout: time.Duration(time.Second * 30),
}
