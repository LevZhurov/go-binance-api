package binance_api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
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
	start, end := startTime.Truncate(time.Second), endTime.Truncate(time.Second)
	list := queryCandlestickSql(symbol, interval, start, end)

	if list == nil {
		list = queryRange(symbol, interval, start, end)
	} else {
		//определяем отсутствующие диапазоны
		intervalDuration := getTimeIntervalDuration(interval)
		if intervalDuration == 0 {
			return nil
		}
		last := start
		var emptyStart, emptyEnd time.Time
		for i, c := range list {
			if last != c.OpenTime {
				if emptyStart.IsZero() {
					emptyStart = last
				}
				emptyEnd = last
			} else {
				if !emptyStart.IsZero() {
					list = append(
						append(list[i:], queryRange(symbol, interval, emptyStart, emptyEnd)...),
						list[:i]...,
					)

					emptyStart, emptyEnd = time.Time{}, time.Time{}
				}
			}
			last = last.Add(intervalDuration)
		}
	}

	//TODO проверить что сортировка не нужна в случае когда добавляли элементы
	//TODO сортировать
	return list
}

func getTimeIntervalDuration(interval TimeIntervals) time.Duration {
	//TODO test
	switch interval {
	case TI_1m:
		return time.Minute
	case TI_5m:
		return 5 * time.Minute
	case TI_15m:
		return 15 * time.Minute
	case TI_30m:
		return 30 * time.Minute
	case TI_1h:
		return time.Hour
	case TI_2h:
		return 2 * time.Hour
	case TI_4h:
		return 4 * time.Hour
	case TI_6h:
		return 6 * time.Hour
	case TI_8h:
		return 8 * time.Hour
	case TI_12h:
		return 12 * time.Hour
	case TI_1d:
		return 24 * time.Hour
	case TI_3d:
		return 3 * 24 * time.Hour
	case TI_1w:
		return 7 * 24 * time.Hour
	case TI_1M:
		return 30 * 24 * time.Hour
	default:
		log.Println("interval is not define", string(debug.Stack()))
		return 0
	}
}

func queryRange(symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
	//TODO test
	startRange := startTime
	intervalDuration := getTimeIntervalDuration(interval)
	if intervalDuration == 0 {
		return nil
	}
	endRange := startRange.Add(1000 * intervalDuration)
	limit := 1000
	list := []*Candlestick{}
	for !endRange.After(endTime) {
		l := queryCandlestickRange(symbol, interval, startRange, endRange, limit)
		if l == nil {
			return list
		}
		list = append(list, l...)
		startRange = endRange
		endRange = startRange.Add(1000 * intervalDuration)
	}

	limit = int((endTime.Sub(startRange) / intervalDuration).Nanoseconds()) + 1
	l := queryCandlestickRange(symbol, interval, startRange, endTime, limit)
	if l == nil {
		return list
	}
	list = append(list, l...)

	//сохранение в БД
	for _, c := range list {
		saveCandlestick(c)
	}

	return list
}

func queryCandlestickRange(symbol string, interval TimeIntervals, startRange, endRange time.Time, limit int) []*Candlestick {
	//TODO test
	retryAfter := "try"
	var list []*Candlestick
	for retryAfter != "" {
		list, _, retryAfter = tryQueryCandlestickRange(symbol, interval, startRange, endRange, limit)
		if retryAfter != "" {
			after, err := strconv.Atoi(retryAfter)
			if err != nil {
				log.Println(err, string(debug.Stack()))
				return nil
			}
			log.Println("retryAfter", retryAfter)
			time.Sleep(time.Duration(after) * time.Second)
		}
	}

	return list
}

func queryCandlestickSql(symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
	//TODO test

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		"root", "kakashulka", "127.0.0.1", "3306", "binance"))
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return nil
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT open_time, open, high, low, close, volume, close_time FROM candlestick WHERE symbol=? AND interval=? AND startTime BETWEEN ? AND ?",
		symbol, string(interval), startTime, endTime)
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return nil
	}
	defer rows.Close()

	list := []*Candlestick{}
	for rows.Next() {
		candle := &struct {
			OpenTime  time.Time
			Open      float64
			High      float64
			Low       float64
			Close     float64
			Volume    float64
			CloseTime time.Time
		}{}
		err := rows.Scan(candle)
		if err != nil {
			log.Println(err, string(debug.Stack()))
			return nil
		}
		list = append(list, &Candlestick{
			OpenTime:  candle.OpenTime,
			Open:      candle.Open,
			High:      candle.High,
			Low:       candle.Low,
			Close:     candle.Close,
			Volume:    candle.Volume,
			CloseTime: candle.CloseTime,
		})
	}
	return list
}

func saveCandlestick(c *Candlestick) {
	//TODO test
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		"root", "kakashulka", "127.0.0.1", "3306", "binance"))
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return
	}
	defer tx.Commit()

	stmt, err := tx.PrepareContext(ctx,
		"INSERT candlestick(open_time, open, high, low, close, volume, close_time) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return
	}
	stmt.Close()

	_, err = stmt.ExecContext(ctx, c.OpenTime, c.Open, c.High, c.Low, c.Close, c.Volume, c.CloseTime)
	if err != nil {
		log.Println(err, string(debug.Stack()))
	}
}

func tryQueryCandlestickRange(symbol string, interval TimeIntervals, startTime, endTime time.Time, limit int) (list []*Candlestick, usedWeight, retryAfter string) {
	//TODO test
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
