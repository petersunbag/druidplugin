package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
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

func (t *DruidDatasource) Query(ctx context.Context, tsdbReq *datasource.DatasourceRequest) (*datasource.DatasourceResponse, error) {
	t.logger.Debug("Query", "datasource", tsdbReq.Datasource.Name, "TimeRange", tsdbReq.TimeRange)

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

	//Input data

	var FromEpochMs, error1 = strconv.ParseInt(strconv.FormatInt(tsdbReq.TimeRange.FromEpochMs, 10)[0:10], 10, 64)
	var ToEpochMs, error2 = strconv.ParseInt(strconv.FormatInt(tsdbReq.TimeRange.ToEpochMs, 10)[0:10], 10, 64)
	if error1 != nil {
	}
	if error2 != nil {
	}

	/*	loc,error3 := time.LoadLocation("America/Los_Angeles")
		if error3!=nil{}*/

	from := time.Unix(int64(FromEpochMs), 0).Format(time.RFC3339)[0:19] + "Z"
	to := time.Unix(int64(ToEpochMs), 0).Format(time.RFC3339)[0:19] + "Z"

	t.logger.Debug("Query", "Intervals", from)
	t.logger.Debug("Query", "Intervals", to)

	modelJson, jsonerr := simplejson.NewJson([]byte(tsdbReq.Queries[0].ModelJson))
	if jsonerr != nil {
	}

	t.logger.Debug("Query", "Model Json", modelJson)

	queryType := modelJson.Get("queryType").MustString()
	datasourceName := modelJson.Get("druidDS").MustString()
	granularity := modelJson.Get("customGranularity").MustString()

	aggregation_name := modelJson.Get("aggregators").GetIndex(0).Get("name").MustString()
	aggregation_type := modelJson.Get("aggregators").GetIndex(0).Get("type").MustString()
	aggregation_field_name := modelJson.Get("aggregators").GetIndex(0).Get("fieldName").MustString()

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
		Aggregations: []Aggregation{{aggregation_type, aggregation_field_name, aggregation_name}},
		Intervals:    []string{from + "/" + to},
		Filters:      Filter{filter.Type, filter.Dimension, filter.Value, filter.Fields},
	}

	/*data := Payload{
		"timeseries",
		"raw_events",
		"minute",
		[]Aggregation{{"ct","count"}},
		[]string{"2019-01-22T09:31:00.000Z/2019-01-22T09:35:49.065Z"},
	}
	*/
	payloadBytes, err := json.Marshal(data)

	if err != nil {
		println("ererrrrrr")
		println(err)
	}

	reqBody := string(payloadBytes)
	if filters_len == 0 {
		reg := regexp.MustCompile(`,"filter":{}`)
		reqBody = reg.ReplaceAllString(reqBody, "")
	}

	t.logger.Error("Query", "Payload", reqBody)

	url := "http://10.151.157.167:8082/druid/v2/?pretty"

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
		return nil, fmt.Errorf("invalid status code. status: %v", resp.Status)
	}

	//Response

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
			Value:     result.(map[string]interface{})[aggregation_name].(float64),
		})

	}

	qr.Series = append(qr.Series, serie)
	response := &datasource.DatasourceResponse{}
	response.Results = append(response.Results, &qr)

	return response, nil

}

func (t *DruidDatasource) createRequest(tsdbReq *datasource.DatasourceRequest) (*remoteDatasourceRequest, error) {
	jQueries := make([]*simplejson.Json, 0)
	for _, query := range tsdbReq.Queries {
		json, err := simplejson.NewJson([]byte(query.ModelJson))
		if err != nil {
			return nil, err
		}

		jQueries = append(jQueries, json)
	}

	queryType := "query"
	if len(jQueries) > 0 {
		queryType = jQueries[0].Get("queryType").MustString("query")
	}

	t.logger.Debug("createRequest", "queryType", queryType)

	payload := simplejson.New()

	switch queryType {
	case "search":
		payload.Set("target", jQueries[0].Get("target").MustString())
	default:
		payload.SetPath([]string{"range", "to"}, tsdbReq.TimeRange.ToRaw)
		payload.SetPath([]string{"range", "from"}, tsdbReq.TimeRange.FromRaw)
		payload.Set("targets", jQueries)
	}

	rbody, err := payload.MarshalJSON()
	if err != nil {
		return nil, err
	}

	url := tsdbReq.Datasource.Url + "/druid/v2"
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(rbody)))
	if err != nil {
		return nil, err
	}

	//if tsdbReq.Datasource.BasicAuth {
	//	req.SetBasicAuth(
	//		tsdbReq.Datasource.BasicAuthUser,
	//		tsdbReq.Datasource.BasicAuthPassword)
	//}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	return &remoteDatasourceRequest{
		queryType: queryType,
		req:       req,
		queries:   jQueries,
	}, nil
}

func (t *DruidDatasource) parseQueryResponse(queries []*simplejson.Json, body []byte, from int64, to int64) (*datasource.DatasourceResponse, error) {
	response := &datasource.DatasourceResponse{}
	responseBody := []TargetResponseDTO{}
	err := json.Unmarshal(body, &responseBody)
	if err != nil {
		return nil, err
	}

	for i, r := range responseBody {
		refId := r.Target

		if len(queries) > i {
			refId = queries[i].Get("refId").MustString()
		}

		qr := datasource.QueryResult{
			RefId:  refId,
			Series: make([]*datasource.TimeSeries, 0),
			Tables: make([]*datasource.Table, 0),
		}

		if len(r.Columns) > 0 {
			table := datasource.Table{
				Columns: make([]*datasource.TableColumn, 0),
				Rows:    make([]*datasource.TableRow, 0),
			}

			for _, c := range r.Columns {
				table.Columns = append(table.Columns, &datasource.TableColumn{
					Name: c.Text,
				})
			}

			for _, row := range r.Rows {
				values := make([]*datasource.RowValue, 0)

				for i, cell := range row {
					rv := datasource.RowValue{}

					switch r.Columns[i].Type {
					case "time":
						if timeValue, ok := cell.(float64); ok {
							rv.Int64Value = int64(timeValue)
						}
						rv.Kind = datasource.RowValue_TYPE_INT64
					case "number":
						if numberValue, ok := cell.(float64); ok {
							rv.Int64Value = int64(numberValue)
						}
						rv.Kind = datasource.RowValue_TYPE_INT64
					case "string":
						if stringValue, ok := cell.(string); ok {
							rv.StringValue = stringValue
						}
						rv.Kind = datasource.RowValue_TYPE_STRING
					default:
						t.logger.Debug(fmt.Sprintf("failed to parse value %v of type %T", cell, cell))
					}

					values = append(values, &rv)
				}

				table.Rows = append(table.Rows, &datasource.TableRow{Values: values})
			}

			qr.Tables = append(qr.Tables, &table)
		} else {
			serie := &datasource.TimeSeries{Name: r.Target}

			for _, p := range r.DataPoints {
				if int64(p[1]) >= from && int64(p[1]) <= to {
					serie.Points = append(serie.Points, &datasource.Point{
						Timestamp: int64(p[1]),
						Value:     p[0],
					})
				}
			}

			qr.Series = append(qr.Series, serie)
		}

		response.Results = append(response.Results, &qr)
	}

	return response, nil
}

func (t *DruidDatasource) parseSearchResponse(body []byte) (*datasource.DatasourceResponse, error) {
	jBody, err := simplejson.NewJson(body)
	if err != nil {
		return nil, err
	}

	metricCount := len(jBody.MustArray())
	table := datasource.Table{
		Columns: []*datasource.TableColumn{
			&datasource.TableColumn{Name: "text"},
		},
		Rows: make([]*datasource.TableRow, 0),
	}

	for n := 0; n < metricCount; n++ {
		values := make([]*datasource.RowValue, 0)
		jm := jBody.GetIndex(n)

		if text, found := jm.CheckGet("text"); found {
			values = append(values, &datasource.RowValue{
				Kind:        datasource.RowValue_TYPE_STRING,
				StringValue: text.MustString(),
			})
			values = append(values, &datasource.RowValue{
				Kind:       datasource.RowValue_TYPE_INT64,
				Int64Value: jm.Get("value").MustInt64(),
			})

			if len(table.Columns) == 1 {
				table.Columns = append(table.Columns, &datasource.TableColumn{Name: "value"})
			}
		} else {
			values = append(values, &datasource.RowValue{
				Kind:        datasource.RowValue_TYPE_STRING,
				StringValue: jm.MustString(),
			})
		}

		table.Rows = append(table.Rows, &datasource.TableRow{Values: values})
	}

	return &datasource.DatasourceResponse{
		Results: []*datasource.QueryResult{
			&datasource.QueryResult{
				RefId:  "search",
				Tables: []*datasource.Table{&table},
			},
		},
	}, nil
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
