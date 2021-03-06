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
	"sort"
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

type (
	binance struct {
		db                    databaser
		site                  binanceSiter
		queryRange            func(log logger, bin *binance, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick
		queryCandlestickRange func(log logger, bin *binance, symbol string, interval TimeIntervals, startRange, endRange time.Time, limit int) []*Candlestick
	}
	Binancer interface {
		QueryCandlestickList(log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick
		Close()
	}
)

func NewBinaceHandler() Binancer {
	return &binance{
		db:                    newBinanceMysqlDatabase("root", "kakashulka", "127.0.0.1", "3306", "binance"),
		site:                  newBinanceSite(),
		queryRange:            queryRange,
		queryCandlestickRange: queryCandlestickRange,
	}
}

func (b *binance) Close() {
	if b == nil {
		log.Println("b *binance is nil", string(debug.Stack()))
		return
	}
	if b.db != nil {
		b.db.close()
	}
}

func (b *binance) QueryCandlestickList(log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
	if b == nil {
		log.Println("b *binance is nil", string(debug.Stack()))
		return nil
	}
	if b.db == nil {
		log.Println("b.db databaser is nil", string(debug.Stack()))
		return nil
	}
	if b.queryRange == nil {
		log.Println("b.queryRange func is nil", string(debug.Stack()))
		return nil
	}

	start, end := startTime.Truncate(time.Second), endTime.Truncate(time.Second)
	list := b.db.queryCandlestickSql(log, symbol, interval, start, end)

	if len(list) == 0 {
		list = b.queryRange(log, b, symbol, interval, start, end)

		if len(list) > 0 {
			//???????????????????? ?? ????
			b.db.saveCandlestick(log, interval, symbol, list)
		}
	} else {
		//???????????????????? ?????????????????????????? ??????????????????
		intervalDuration := getTimeIntervalDuration(log, interval)
		if intervalDuration == 0 {
			return nil
		}

		//?? ???????????? ??????????????????
		if start.Before(list[0].OpenTime) {
			//?????????????????????? ?????????? ?????????????? ??????????????????
			rangeList := b.queryRange(log, b, symbol, interval, start, list[0].OpenTime.Add(-intervalDuration))
			if len(rangeList) > 0 {
				//???????????????????? ?? ????
				b.db.saveCandlestick(log, interval, symbol, rangeList)

				list = append(rangeList, list...)
			}
		}

		last := list[0].OpenTime

		for i, c := range list {
			if last.Before(c.OpenTime) {
				//???????????????? ?????????????? ?????????????? ??????????????????
				emptyStart := last
				for last.Before(end) && last.Before(c.OpenTime) {
					last = last.Add(intervalDuration)
				}
				emptyEnd := last.Add(-intervalDuration)

				//?????????????????????? ?????????? ?????????????? ??????????????????
				rangeList := b.queryRange(log, b, symbol, interval, emptyStart, emptyEnd)
				if len(rangeList) > 0 {
					//???????????????????? ?? ????
					b.db.saveCandlestick(log, interval, symbol, rangeList)

					list = append(
						append(list[i:], rangeList...),
						list[:i]...,
					)
				}
			}

			last = last.Add(intervalDuration)
		}

		//?? ?????????? ??????????????????
		if last.Before(end) {
			//?????????????????????? ?????????? ?????????????? ??????????????????
			rangeList := b.queryRange(log, b, symbol, interval, last, end)

			if len(rangeList) > 0 {
				//???????????????????? ?? ????
				b.db.saveCandlestick(log, interval, symbol, rangeList)

				list = append(list, rangeList...)
			}
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].OpenTime.Before(list[j].OpenTime)
	})

	return list
}

type (
	binanceDatabase struct {
		db *sql.DB
	}
	databaser interface {
		queryCandlestickSql(log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick
		saveCandlestick(log logger, interval TimeIntervals, symbol string, list []*Candlestick) error
		close()
	}
)

func newBinanceMysqlDatabase(user, passwd, addr, port, dbname string) databaser {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		user, passwd, addr, port, dbname))
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return nil
	}
	return &binanceDatabase{
		db: db,
	}
}
func (bdb *binanceDatabase) close() {
	if bdb == nil {
		log.Println("bdb *binanceDatabase is nil", string(debug.Stack()))
		return
	}
	if bdb.db == nil {
		return
	}
	bdb.db.Close()
}
func (bdb *binanceDatabase) queryCandlestickSql(log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
	if bdb == nil {
		log.Println("bdb *binanceDatabase is nil", string(debug.Stack()))
		return nil
	}
	if bdb.db == nil {
		log.Println("bdb.db *sql.DB is nil", string(debug.Stack()))
		return nil
	}
	log.Printf("queryCandlestickSql startTime %v, endTime %v",
		startTime, endTime,
	)

	err := bdb.db.Ping()
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := bdb.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT open_time, open, high, low, close, volume, close_time "+
			"FROM candlestick "+
			"WHERE symbol='%s' AND `interval`='%s' AND open_time BETWEEN '%v' AND '%v' "+
			"ORDER BY open_time ASC;",
		symbol,
		interval,
		startTime.In(time.UTC).Format("2006-01-02 15:04:05"),
		endTime.In(time.UTC).Format("2006-01-02 15:04:05"),
	))
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return nil
	}
	defer rows.Close()

	list := []*Candlestick{}
	for rows.Next() {
		var (
			OpenTime  string
			Open      float64
			High      float64
			Low       float64
			Close     float64
			Volume    float64
			CloseTime string
		)
		err := rows.Scan(&OpenTime, &Open, &High, &Low, &Close, &Volume, &CloseTime)
		if err != nil {
			log.Println(err, string(debug.Stack()))
			return nil
		}

		ot, err := time.ParseInLocation("2006-01-02 15:04:05", OpenTime, time.UTC)
		if err != nil {
			log.Println(err, string(debug.Stack()))
			return nil
		}
		ct, err := time.ParseInLocation("2006-01-02 15:04:05", CloseTime, time.UTC)
		if err != nil {
			log.Println(err, string(debug.Stack()))
			return nil
		}
		list = append(list, &Candlestick{
			OpenTime:  ot.In(time.Local),
			Open:      Open,
			High:      High,
			Low:       Low,
			Close:     Close,
			Volume:    Volume,
			CloseTime: ct.In(time.Local),
		})
	}

	if len(list) > 0 {
		log.Printf("queryCandlestickSql return len(%v) start %v, end %v",
			len(list), list[0].OpenTime, list[len(list)-1].OpenTime,
		)
	}
	return list
}
func (bdb *binanceDatabase) saveCandlestick(log logger, interval TimeIntervals, symbol string, list []*Candlestick) error {
	if bdb == nil {
		log.Println("bdb *binanceDatabase is nil", string(debug.Stack()))
		return nil
	}
	if bdb.db == nil {
		log.Println("bdb.db *sql.DB is nil", string(debug.Stack()))
		return nil
	}
	log.Printf("saveCandlestick  len(%v) start %v, end %v",
		len(list), list[0].OpenTime, list[len(list)-1].OpenTime,
	)

	err := bdb.db.Ping()
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tx, err := bdb.db.BeginTx(ctx, nil)
	if err != nil {
		log.Println(err, string(debug.Stack()))
		return err
	}
	defer tx.Rollback()

	var kol int
	args := []interface{}{}
	var textArgs string
	prevCandle := &Candlestick{}
	for _, c := range list {
		if prevCandle.OpenTime.Equal(c.OpenTime) {
			log.Printf("Candle duplicate prev %v current %v", prevCandle, c)
			continue
		}
		prevCandle = c
		if kol > 100 {
			_, err = tx.ExecContext(ctx,
				"INSERT INTO candlestick(`interval`, symbol, open_time, open, close, high, low, volume, close_time) "+
					"VALUES "+textArgs[:len(textArgs)-1]+";", args...,
			)
			if err != nil {
				log.Println(err, string(debug.Stack()))
				return err
			}

			args = []interface{}{}
			textArgs = ""
		}

		args = append(args,
			interval,
			symbol,
			c.OpenTime.In(time.UTC).Format("2006-01-02 15:04:05"),
			c.Open,
			c.Close,
			c.High,
			c.Low,
			c.Volume,
			c.CloseTime.In(time.UTC).Format("2006-01-02 15:04:05"),
		)
		textArgs += "(?, ?, ?, ?, ?, ?, ?, ?, ?),"

		kol++
	}

	if kol > 0 {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO candlestick(`interval`, symbol, open_time, open, close, high, low, volume, close_time) "+
				"VALUES "+textArgs[:len(textArgs)-1]+";", args...,
		)
		if err != nil {
			log.Println(err, string(debug.Stack()))
			return err
		}
	}

	return tx.Commit()
}

func getTimeIntervalDuration(log logger, interval TimeIntervals) time.Duration {
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

func queryRange(log logger, bin *binance, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
	if bin == nil {
		log.Println("bin *binance is nil", string(debug.Stack()))
		return nil
	}
	if bin.queryCandlestickRange == nil {
		log.Println("bin.queryCandlestickRange func is nil", string(debug.Stack()))
		return nil
	}
	if log == nil {
		log.Println("log logger is nil", string(debug.Stack()))
		return nil
	}
	log.Printf("queryRange startTime %v, endTime %v",
		startTime, endTime,
	)

	startRange := startTime
	intervalDuration := getTimeIntervalDuration(log, interval)
	if intervalDuration == 0 {
		return nil
	}
	endRange := startRange.Add(1000 * intervalDuration)
	limit := 1000
	list := []*Candlestick{}
	for !endRange.After(endTime) {
		l := bin.queryCandlestickRange(log, bin, symbol, interval, startRange, endRange, limit)
		if l == nil {
			return list
		}
		list = append(list, l...)
		startRange = endRange
		endRange = startRange.Add(1000 * intervalDuration)
	}

	limit = int((endTime.Sub(startRange) / intervalDuration).Nanoseconds()) + 1
	l := bin.queryCandlestickRange(log, bin, symbol, interval, startRange, endTime, limit)
	if l == nil {
		return list
	}
	list = append(list, l...)

	if len(list) > 0 {
		log.Printf("queryRange return len(%v) start %v, end %v",
			len(list), list[0].OpenTime, list[len(list)-1].OpenTime,
		)
	}
	return list
}

func queryCandlestickRange(log logger, bin *binance, symbol string, interval TimeIntervals, startRange, endRange time.Time, limit int) []*Candlestick {
	if bin == nil {
		log.Println("bin *binance is nil", string(debug.Stack()))
		return nil
	}
	if bin.site == nil {
		log.Println("bin.site binanceSiter is nil", string(debug.Stack()))
		return nil
	}
	retryAfter := "try"
	var list []*Candlestick
	for retryAfter != "" {
		list, _, retryAfter = bin.site.tryQueryCandlestickRange(log, symbol, interval, startRange, endRange, limit)
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

type (
	binanceSite struct {
		client           *http.Client
		addressApi       string
		queryCandlestick string
	}
	binanceSiter interface {
		tryQueryCandlestickRange(log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time, limit int) (list []*Candlestick, usedWeight, retryAfter string)
	}
)

func newBinanceSite() *binanceSite {
	return &binanceSite{
		client:           &http.Client{},
		addressApi:       "https://api.binance.com/",
		queryCandlestick: "api/v3/klines",
	}
}
func (bs *binanceSite) tryQueryCandlestickRange(log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time, limit int) (list []*Candlestick, usedWeight, retryAfter string) {
	var param string

	if symbol == "" {
		log.Println("symbol ???? ?????????? ???????? ????????????", string(debug.Stack()))
		return
	}
	param += "?symbol=" + symbol
	if interval == "" {
		log.Println("interval ???? ?????????? ???????? ????????????", string(debug.Stack()))
		return
	}
	param += "&interval=" + string(interval)
	if !startTime.IsZero() {
		param += "&startTime=" + fmt.Sprintf("%v", startTime.In(time.UTC).UnixNano()/int64(time.Millisecond))
	}
	if !endTime.IsZero() {
		param += "&endTime=" + fmt.Sprintf("%v", endTime.In(time.UTC).UnixNano()/int64(time.Millisecond))
	}
	if limit != 0 {
		param += "&limit=" + fmt.Sprintf("%v", limit)
	}

	//https://api.binance.com/api/v3/klines
	resp, err := bs.client.Get(bs.addressApi + bs.queryCandlestick + param)
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

func parseCandlestick(items []interface{}) *Candlestick {
	if len(items) < 12 {
		log.Println("len(items []interface{}) < 12")
		return nil
	}
	lc := newLogCollector()
	candlestick := &Candlestick{
		OpenTime:                 parseInterfaceToTime(lc, items[0]),
		Open:                     parseInterfaceToFloat64(lc, items[1]),
		High:                     parseInterfaceToFloat64(lc, items[2]),
		Low:                      parseInterfaceToFloat64(lc, items[3]),
		Close:                    parseInterfaceToFloat64(lc, items[4]),
		Volume:                   parseInterfaceToFloat64(lc, items[5]),
		CloseTime:                parseInterfaceToTime(lc, items[6]),
		QuoteAssetVolume:         parseInterfaceToFloat64(lc, items[7]),
		NumberOfTrades:           parseInterfaceToInt(lc, items[8]),
		TakerBuyBaseAssetVolume:  parseInterfaceToFloat64(lc, items[9]),
		TakerBuyQuoteAssetVolume: parseInterfaceToFloat64(lc, items[10]),
		Ignore:                   parseInterfaceToFloat64(lc, items[11]),
	}
	if len(lc.logs) > 0 {
		log.Println(lc)
		return nil
	}
	return candlestick
}

func parseInterfaceToTime(log logger, val interface{}) time.Time {
	return time.Unix(0, parseInterfaceToInt64(log, val)*int64(time.Millisecond))
}

func parseInterfaceToInt64(log logger, val interface{}) int64 {
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
		if float64(int64(v)) == v {
			return int64(v)
		}
		log.Printf("(%T) %v != (int64)\n%s", val, val, string(debug.Stack()))
		return 0
	default:
		log.Printf("(%T) %v != (int64)\n%s", val, val, string(debug.Stack()))
		return 0
	}
}

func parseInterfaceToFloat64(log logger, val interface{}) float64 {
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
	switch v := val.(type) {
	case int:
		return v
	case float64:
		if float64(int(v)) == v {
			return int(v)
		}
		log.Printf("(%T) %v != (int)\n%s", val, val, string(debug.Stack()))
		return 0
	default:
		log.Printf("(%T) %v != (int)\n%s", val, val, string(debug.Stack()))
		return 0
	}
}
