package binance_api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"
)

type (
	Candlestick struct {
		OpenTime                 time.Time
		Open                     float64
		High                     float64
		Low                      float64
		Close                    float64
		Volume                   float64
		CloseTime                time.Time
		QuoteAssetVolume         float64
		NumberOfTrades           int
		TakerBuyBaseAssetVolume  float64
		TakerBuyQuoteAssetVolume float64
		Ignore                   float64
	}
	TimeIntervals string
)

const (
	TI_1m  TimeIntervals = "1m"
	TI_5m  TimeIntervals = "5m"
	TI_15m TimeIntervals = "15m"
	TI_30m TimeIntervals = "30m"
	TI_1h  TimeIntervals = "1h"
	TI_2h  TimeIntervals = "2h"
	TI_4h  TimeIntervals = "4h"
	TI_6h  TimeIntervals = "6h"
	TI_8h  TimeIntervals = "8h"
	TI_12h TimeIntervals = "12h"
	TI_1d  TimeIntervals = "1d"
	TI_3d  TimeIntervals = "3d"
	TI_1w  TimeIntervals = "1w"
	TI_1M  TimeIntervals = "1M"
)

func QueryCandlestickList(symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
	limit := 1000
	list := []*Candlestick{}

	start, end := startTime.Truncate(time.Second), endTime.Truncate(time.Second)
	startRange := start
	var intervalDuration time.Duration
	switch interval {
	case TI_1m:
		intervalDuration = time.Minute
	case TI_5m:
		intervalDuration = 5 * time.Minute
	case TI_15m:
		intervalDuration = 15 * time.Minute
	case TI_30m:
		intervalDuration = 30 * time.Minute
	case TI_1h:
		intervalDuration = time.Hour
	case TI_2h:
		intervalDuration = 2 * time.Hour
	case TI_4h:
		intervalDuration = 4 * time.Hour
	case TI_6h:
		intervalDuration = 6 * time.Hour
	case TI_8h:
		intervalDuration = 8 * time.Hour
	case TI_12h:
		intervalDuration = 12 * time.Hour
	case TI_1d:
		intervalDuration = 24 * time.Hour
	case TI_3d:
		intervalDuration = 3 * 24 * time.Hour
	case TI_1w:
		intervalDuration = 7 * 24 * time.Hour
	case TI_1M:
		intervalDuration = 30 * 24 * time.Hour
	default:
		log.Println("interval is not define", string(debug.Stack()))
		return nil
	}
	endRange := startRange.Add(1000 * intervalDuration)

	queryRange := func(symbol string, interval TimeIntervals, startTime, endTime time.Time, limit int) []*Candlestick {
		retryAfter := "try"
		var list []*Candlestick
		for retryAfter != "" {
			list, _, retryAfter = queryCandlestickRange(symbol, interval, startRange, endRange, limit)
			if retryAfter != "" {
				after, err := strconv.Atoi(retryAfter)
				if err != nil {
					log.Println(err, string(debug.Stack()))
					return nil
				}
				time.Sleep(time.Duration(after) * time.Second)
			}
		}
		return list
	}

	for !endRange.After(end) {
		l := queryRange(symbol, interval, startRange, endRange, limit)
		if l == nil {
			return list
		}
		list = append(list, l...)
		startRange = endRange
		endRange = startRange.Add(1000 * intervalDuration)
	}

	limit = int((end.Sub(startRange) / intervalDuration).Nanoseconds()) + 1
	l := queryRange(symbol, interval, startRange, end, limit)
	if l == nil {
		return list
	}
	list = append(list, l...)

	return list
}

func queryCandlestickRange(symbol string, interval TimeIntervals, startTime, endTime time.Time, limit int) (list []*Candlestick, usedWeight, retryAfter string) {
	var param string

	if symbol == "" {
		log.Println("symbol не может быть пустым", string(debug.Stack()))
		return
	}
	param += "?symbol=" + symbol
	if interval == "" {
		log.Println("interval не может быть пустым", string(debug.Stack()))
		return
	}
	param += "&interval=" + string(interval)
	if !startTime.IsZero() {
		param += "&startTime=" + fmt.Sprintf("%v", startTime.UnixNano()/int64(time.Millisecond))
	}
	if !endTime.IsZero() {
		param += "&endTime=" + fmt.Sprintf("%v", endTime.UnixNano()/int64(time.Millisecond))
	}
	if limit != 0 {
		param += "&limit=" + fmt.Sprintf("%v", limit)
	}

	//TODO брать client и адрес запроса из ресивера этого метода для безопасных тестов
	client := http.Client{}
	resp, err := client.Get("https://api.binance.com/api/v3/klines" + param)
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return
	}

	usedWeight = resp.Header.Get("X-Mbx-Used-Weight")
	retryAfter = resp.Header.Get("Retry-After")

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return
	}

	prepareBody := []byte(`{"list":`)
	prepareBody = append(prepareBody, respBody...)
	prepareBody = append(prepareBody, []byte(`}`)...)
	data := struct {
		List [][]interface{} `json:"list"`
	}{}
	err = json.Unmarshal(prepareBody, &data)
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return
	}

	list = make([]*Candlestick, 0, len(data.List))
	for _, item := range data.List {
		list = append(list, parseCandlestick(item))
	}

	return
}

func parseCandlestick(item []interface{}) *Candlestick {
	lc := newLogCollector()
	candlestick := &Candlestick{
		OpenTime:                 time.Unix(0, parseInterfaceToInt64(lc, item[0])*int64(time.Millisecond)),
		Open:                     parseInterfaceToFloat64(lc, item[1]),
		High:                     parseInterfaceToFloat64(lc, item[2]),
		Low:                      parseInterfaceToFloat64(lc, item[3]),
		Close:                    parseInterfaceToFloat64(lc, item[4]),
		Volume:                   parseInterfaceToFloat64(lc, item[5]),
		CloseTime:                time.Unix(0, parseInterfaceToInt64(lc, item[6])*int64(time.Millisecond)),
		QuoteAssetVolume:         parseInterfaceToFloat64(lc, item[7]),
		NumberOfTrades:           parseInterfaceToInt(lc, item[8]),
		TakerBuyBaseAssetVolume:  parseInterfaceToFloat64(lc, item[9]),
		TakerBuyQuoteAssetVolume: parseInterfaceToFloat64(lc, item[10]),
		Ignore:                   parseInterfaceToFloat64(lc, item[11]),
	}
	if len(lc.logs) > 0 {
		log.Println(lc)
		return nil
	}
	return candlestick
}

func parseInterfaceToInt64(log logger, val interface{}) int64 {
	//TODO test
	switch v := val.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		log.Printf("(%T) %v != (int64)\n%s", val, val, string(debug.Stack()))
		return 0
	}
}

func parseInterfaceToFloat64(log logger, val interface{}) float64 {
	//TODO test
	r, ok := val.(string)
	if !ok {
		log.Printf("(%T) %v != (string)\n%s", val, val, string(debug.Stack()))
		return 0
	}
	result, err := strconv.ParseFloat(r, 64)
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return 0
	}
	return result
}

func parseInterfaceToInt(log logger, val interface{}) int {
	//TODO test
	switch v := val.(type) {
	case int:
		return v
	case float64:
		return int(v)
	default:
		log.Printf("(%T) %v != (int)\n%s", val, val, string(debug.Stack()))
		return 0
	}
}
