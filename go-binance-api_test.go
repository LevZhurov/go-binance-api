package binance_api

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
	_ "unsafe"

	"github.com/LevZhurov/autotest"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

var testlog = newLogCollector()

func Test_prepare(t *testing.T) {
	log.SetOutput(ioutil.Discard)
}

type binanceDatabaseTestImplementer struct{}

func (bdb *binanceDatabaseTestImplementer) queryCandlestickSql(log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
	return nil
}
func (bdb *binanceDatabaseTestImplementer) saveCandlestick(log logger, interval TimeIntervals, symbol string, list []*Candlestick) error {
	return nil
}
func (bdb *binanceDatabaseTestImplementer) close() {}

//go:linkname t_binanceDatabase_close github.com/LevZhurov/go-binance-api.(*binanceDatabase).close
func t_binanceDatabase_close(bdb *binanceDatabase)
func Test_binanceDatabase_close(t *testing.T) {
	autotest.FunctionTesting(t, t_binanceDatabase_close)
}

func Test_NewBinaceHandler(t *testing.T) {
	autotest.FunctionTesting(t, NewBinaceHandler)

	bin := NewBinaceHandler().(*binance)
	require.NotNil(t, bin.db)
	require.NotNil(t, bin.site)
	require.NotNil(t, bin.queryRange)
	require.NotNil(t, bin.queryCandlestickRange)
}

type (
	database_binance_QueryCandlestickList struct {
		binanceDatabaseTestImplementer
		list     []*Candlestick
		saveList []*Candlestick
	}
)

func (db *database_binance_QueryCandlestickList) queryCandlestickSql(log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
	return db.list
}
func (db *database_binance_QueryCandlestickList) saveCandlestick(log logger, interval TimeIntervals, symbol string, list []*Candlestick) error {
	db.saveList = append(db.saveList, list...)
	return nil
}

//go:linkname t_binance_QueryCandlestickList github.com/LevZhurov/go-binance-api.(*binance).QueryCandlestickList
func t_binance_QueryCandlestickList(b *binance, log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick
func Test_binance_QueryCandlestickList(t *testing.T) {
	autotest.FunctionTesting(t, t_binance_QueryCandlestickList, nil, testlog)

	///
	//в базе пусто
	///
	var isQueryRange bool
	queryRange := func(bin *binance, lo logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
		isQueryRange = true
		return []*Candlestick{
			&Candlestick{Open: 2.5},
		}
	}
	db := &database_binance_QueryCandlestickList{saveList: []*Candlestick{}}
	bin := &binance{
		db:         db,
		queryRange: queryRange,
	}
	lc := newLogCollector()
	list := bin.QueryCandlestickList(lc, "symbol", TI_1m, time.Time{}, time.Time{})
	require.Equal(t, []string{}, lc.logs)
	require.Equal(t, []*Candlestick{
		&Candlestick{Open: 2.5},
	}, list)
	require.True(t, isQueryRange)
	require.Equal(t, []*Candlestick{
		&Candlestick{Open: 2.5},
	}, db.saveList)

	///
	//в базе есть данные полностью
	///
	isQueryRange = false
	queryRange = func(bin *binance, lo logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
		isQueryRange = true
		return []*Candlestick{}
	}
	start, _ := time.Parse("2006-01-02 15:04:05", "2021-09-23 00:00:00")
	end, _ := time.Parse("2006-01-02 15:04:05", "2021-09-23 00:01:00")
	db = &database_binance_QueryCandlestickList{
		list: []*Candlestick{
			&Candlestick{OpenTime: start, Open: 2.5},
			&Candlestick{OpenTime: end, Open: 2.5},
		},
		saveList: []*Candlestick{},
	}
	bin = &binance{
		db:         db,
		queryRange: queryRange,
	}
	lc = newLogCollector()
	list = bin.QueryCandlestickList(lc, "symbol", TI_1m, start, end)
	require.Equal(t, []string{}, lc.logs)
	require.Equal(t, []*Candlestick{
		&Candlestick{OpenTime: start, Open: 2.5},
		&Candlestick{OpenTime: end, Open: 2.5},
	}, list)
	require.False(t, isQueryRange)
	require.Equal(t, []*Candlestick{}, db.saveList)

	///
	//в базе есть данные не полностью
	///
	now, _ := time.Parse("2006-01-02 15:04:05", "2021-08-31 03:00:00")
	now = now.Truncate(24 * time.Hour)
	isQueryRange = false
	queryRange = func(bin *binance, lo logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick {
		//lo.Printf("queryRange start %v end %v\n", startTime, endTime)
		isQueryRange = true
		if startTime.Equal(endTime) {
			return []*Candlestick{
				&Candlestick{OpenTime: startTime},
			}
		}
		var list []*Candlestick
		now := startTime
		for now.Before(endTime) {
			list = append(list, &Candlestick{OpenTime: now})
			now = now.Add(getTimeIntervalDuration(lo, interval))
		}
		list = append(list, &Candlestick{OpenTime: now})
		return list
	}
	db = &database_binance_QueryCandlestickList{
		list: []*Candlestick{
			&Candlestick{OpenTime: now.AddDate(0, 0, 3)},
			&Candlestick{OpenTime: now.AddDate(0, 0, 8)},
			&Candlestick{OpenTime: now.AddDate(0, 0, 11)},
			&Candlestick{OpenTime: now.AddDate(0, 0, 13)},
			&Candlestick{OpenTime: now.AddDate(0, 0, 17)},
		},
		saveList: []*Candlestick{},
	}
	bin = &binance{
		db:         db,
		queryRange: queryRange,
	}
	lc = newLogCollector()
	list = bin.QueryCandlestickList(lc, "symbol", TI_1d, now.AddDate(0, 0, 0), now.AddDate(0, 0, 20))
	require.Equal(t, []string{}, lc.logs)
	require.True(t, isQueryRange)

	expectedList := []*Candlestick{
		&Candlestick{OpenTime: now.AddDate(0, 0, 0)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 1)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 2)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 3)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 4)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 5)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 6)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 7)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 8)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 9)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 10)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 11)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 12)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 13)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 14)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 15)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 16)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 17)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 18)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 19)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 20)},
	}
	for i, c := range list {
		require.True(t, expectedList[i].OpenTime.Equal(c.OpenTime),
			fmt.Sprintf("candle: #%v, expected: %v, actual: %v", i, expectedList[i].OpenTime, c.OpenTime))
		//t.Log(fmt.Sprintf("candle: #%v, expected: %v, actual: %v", i, expectedList[i].OpenTime, c.OpenTime))
	}

	expectedList = []*Candlestick{
		&Candlestick{OpenTime: now.AddDate(0, 0, 0)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 1)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 2)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 4)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 5)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 6)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 7)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 9)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 10)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 12)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 14)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 15)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 16)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 18)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 19)},
		&Candlestick{OpenTime: now.AddDate(0, 0, 20)},
	}
	for i, c := range db.saveList {
		require.True(t, expectedList[i].OpenTime.Equal(c.OpenTime),
			fmt.Sprintf("candle: #%v, expected: %v, actual: %v", i, expectedList[i].OpenTime, c.OpenTime))
	}
}

//go:linkname t_binanceDatabase_queryCandlestickSql github.com/LevZhurov/go-binance-api.(*binanceDatabase).queryCandlestickSql
func t_binanceDatabase_queryCandlestickSql(bdb *binanceDatabase, log logger, symbol string, interval TimeIntervals, startTime, endTime time.Time) []*Candlestick
func Test_binanceDatabase_queryCandlestickSql(t *testing.T) {
	autotest.FunctionTesting(t, t_binanceDatabase_queryCandlestickSql, nil, testlog)

	location, _ := time.LoadLocation("UTC")
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bdb := &binanceDatabase{
		db: db,
	}

	testTable := []struct {
		name         string
		mockBehavior func()
		expectedList []*Candlestick
		wantErr      bool
	}{
		{
			name: "ok",
			mockBehavior: func() {
				rows := sqlmock.NewRows([]string{"open_time", "open", "high", "low", "close", "volume", "close_time"}).
					AddRow("2021-09-30 20:36:00", 43000, 43200, 42300, 42500, 12000, "2021-09-30 20:37:00")
				mock.ExpectQuery("SELECT open_time, open, high, low, close, volume, close_time " +
					"FROM candlestick " +
					"WHERE symbol='BTCUSDT' AND `interval`='1m' AND " +
					"c.open_time BETWEEN '2021-09-30 20:36:00' AND '2021-09-30 20:36:00' " +
					"ORDER BY c.open_time ASC;").
					WillReturnRows(rows)
			},
			expectedList: []*Candlestick{
				&Candlestick{
					OpenTime:  time.Date(2021, 9, 30, 20, 36, 00, 0, location),
					Open:      43000,
					High:      43200,
					Low:       42300,
					Close:     42500,
					Volume:    12000,
					CloseTime: time.Date(2021, 9, 30, 20, 37, 00, 0, location),
				},
			},
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.mockBehavior()

			//TODO поймать ошибку
			// if testCase.wantErr {

			// }
			list := bdb.queryCandlestickSql(
				testlog,
				"BTCUSDT",
				TI_1m,
				time.Date(2021, 9, 30, 20, 36, 00, 0, location),
				time.Date(2021, 9, 30, 20, 36, 00, 0, location),
			)
			for i, c := range list {
				require.True(t, testCase.expectedList[i].OpenTime.Equal(c.OpenTime),
					fmt.Sprintf("candle: #%v, expected: %v, actual: %v",
						i, testCase.expectedList[i].OpenTime, c.OpenTime))
				require.True(t, testCase.expectedList[i].CloseTime.Equal(c.CloseTime),
					fmt.Sprintf("candle: #%v, expected: %v, actual: %v",
						i, testCase.expectedList[i].CloseTime, c.CloseTime))
				require.Equal(t, testCase.expectedList[i].Close, c.Close)
				require.Equal(t, testCase.expectedList[i].High, c.High)
				require.Equal(t, testCase.expectedList[i].Low, c.Low)
				require.Equal(t, testCase.expectedList[i].Open, c.Open)
				require.Equal(t, testCase.expectedList[i].Volume, c.Volume)
			}
		})
	}
}

//go:linkname t_binanceDatabase_saveCandlestick github.com/LevZhurov/go-binance-api.(*binanceDatabase).saveCandlestick
func t_binanceDatabase_saveCandlestick(bdb *binanceDatabase, log logger, interval TimeIntervals, symbol string, list []*Candlestick) error
func Test_binanceDatabase_saveCandlestick(t *testing.T) {
	autotest.FunctionTesting(t, t_binanceDatabase_saveCandlestick, nil, testlog)

	location, _ := time.LoadLocation("UTC")
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bdb := &binanceDatabase{
		db: db,
	}

	testTable := []struct {
		name                 string
		mockBehavior         func()
		inputInterval        TimeIntervals
		inputSymbol          string
		inputCandlestickList []*Candlestick
		wantErr              bool
	}{
		{
			name: "ok",
			mockBehavior: func() {
				mock.ExpectBegin()

				mock.ExpectExec("INSERT INTO candlestick").
					WithArgs(
						"1m",
						"BTCUSDT",
						"2021-09-29 20:36:00",
						43000.0,
						42500.0,
						43200.0,
						42300.0,
						12000.0,
						"2021-09-29 20:37:00",
					).
					WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectCommit()
			},
			inputInterval: TI_1m,
			inputSymbol:   "BTCUSDT",
			inputCandlestickList: []*Candlestick{&Candlestick{
				OpenTime:  time.Date(2021, 9, 29, 20, 36, 00, 0, location),
				Open:      43000,
				High:      43200,
				Low:       42300,
				Close:     42500,
				Volume:    12000,
				CloseTime: time.Date(2021, 9, 29, 20, 37, 00, 0, location),
			}},
		},
		{
			name: "ok multi",
			mockBehavior: func() {
				mock.ExpectBegin()

				mock.ExpectExec("INSERT INTO candlestick").
					WithArgs(
						"1m",
						"BTCUSDT",
						"2021-09-29 20:36:00",
						1.0,
						42500.0,
						43200.0,
						42300.0,
						12000.0,
						"2021-09-29 20:37:00",
						"1m",
						"BTCUSDT",
						"2021-09-29 20:36:00",
						2.0,
						42500.0,
						43200.0,
						42300.0,
						12000.0,
						"2021-09-29 20:37:00",
					).
					WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectCommit()
			},
			inputInterval: TI_1m,
			inputSymbol:   "BTCUSDT",
			inputCandlestickList: []*Candlestick{
				&Candlestick{
					OpenTime:  time.Date(2021, 9, 29, 20, 36, 00, 0, location),
					Open:      1,
					High:      43200,
					Low:       42300,
					Close:     42500,
					Volume:    12000,
					CloseTime: time.Date(2021, 9, 29, 20, 37, 00, 0, location),
				},
				&Candlestick{
					OpenTime:  time.Date(2021, 9, 29, 20, 36, 00, 0, location),
					Open:      2,
					High:      43200,
					Low:       42300,
					Close:     42500,
					Volume:    12000,
					CloseTime: time.Date(2021, 9, 29, 20, 37, 00, 0, location),
				},
			},
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.mockBehavior()

			err := bdb.saveCandlestick(
				testlog,
				testCase.inputInterval,
				testCase.inputSymbol,
				testCase.inputCandlestickList,
			)
			if testCase.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type binanceSite_queryCandlestickRange struct {
	retryAfter []string
	list       [][]*Candlestick
}

func (bs *binanceSite_queryCandlestickRange) tryQueryCandlestickRange(symbol string, interval TimeIntervals, startTime, endTime time.Time, limit int) (list []*Candlestick, usedWeight, retryAfter string) {
	l := bs.list[0]
	bs.list = bs.list[1:]
	ra := bs.retryAfter[0]
	bs.retryAfter = bs.retryAfter[1:]
	return l, "", ra
}

func Test_queryCandlestickRange(t *testing.T) {
	//t.Parallel()
	autotest.FunctionTesting(t, queryCandlestickRange)

	list1 := []*Candlestick{
		&Candlestick{},
	}
	list2 := []*Candlestick{
		&Candlestick{},
		&Candlestick{},
	}
	site := &binanceSite_queryCandlestickRange{
		retryAfter: []string{"1", ""},
		list: [][]*Candlestick{
			list1, list2,
		},
	}
	bin := &binance{
		site:                  site,
		queryCandlestickRange: queryCandlestickRange,
	}
	start := time.Now()
	require.Equal(t, list2, bin.queryCandlestickRange(bin, "", "", time.Time{}, time.Time{}, 0))
	require.True(t, start.Add(time.Second).Before(time.Now()))
}

func Test_queryRange(t *testing.T) {
	autotest.FunctionTesting(t, queryRange, nil, testlog)

	now := time.Now().Truncate(24 * time.Hour)
	list := queryRange(&binance{
		queryCandlestickRange: func(bin *binance, symbol string, interval TimeIntervals, startRange, endRange time.Time, limit int) []*Candlestick {
			list := []*Candlestick{
				&Candlestick{OpenTime: startRange},
				&Candlestick{OpenTime: endRange},
			}
			return list
		},
	}, newLogCollector(), "symbol", TI_1m, now.AddDate(0, 0, -2), now)

	require.Equal(t, 6, len(list))
	require.True(t, now.AddDate(0, 0, -2).Equal(list[0].OpenTime), fmt.Sprintf("expected: %v, actual: %v",
		now.AddDate(0, 0, -2).Add(1000*time.Minute), list[0].OpenTime))

	require.True(t, now.AddDate(0, 0, -2).Add(1000*time.Minute).Equal(list[1].OpenTime), fmt.Sprintf("expected: %v, actual: %v",
		now.AddDate(0, 0, -2).Add(1000*time.Minute), list[1].OpenTime))

	require.True(t, now.AddDate(0, 0, -2).Add(1001*time.Minute).Equal(list[2].OpenTime), fmt.Sprintf("expected: %v, actual: %v",
		now.AddDate(0, 0, -2).Add(1001*time.Minute), list[2].OpenTime))

	require.True(t, now.AddDate(0, 0, -2).Add(2001*time.Minute).Equal(list[3].OpenTime), fmt.Sprintf("expected: %v, actual: %v",
		now.AddDate(0, 0, -2).Add(2001*time.Minute), list[3].OpenTime))

	require.True(t, now.AddDate(0, 0, -2).Add(2002*time.Minute).Equal(list[4].OpenTime), fmt.Sprintf("expected: %v, actual: %v",
		now.AddDate(0, 0, -2).Add(2002*time.Minute), list[4].OpenTime))

	require.True(t, now.Equal(list[5].OpenTime), fmt.Sprintf("expected: %v, actual: %v",
		now, list[5].OpenTime))
}

type binanceDatabase_binance_close struct {
	binanceDatabaseTestImplementer
	isClose bool
}

func (bdb *binanceDatabase_binance_close) close() {
	bdb.isClose = true
}

//go:linkname t_binance_Close github.com/LevZhurov/go-binance-api.(*binance).Close
func t_binance_Close(b *binance)
func Test_binance_close(t *testing.T) {
	autotest.FunctionTesting(t, t_binance_Close)

	db := &binanceDatabase_binance_close{}
	b := &binance{
		db: db,
	}
	b.Close()
	require.True(t, db.isClose)
}

func Test_newBinanceMysqlDatabase(t *testing.T) {
	autotest.FunctionTesting(t, newBinanceMysqlDatabase)

	//TODO заменить реальное подключение моком
	db := newBinanceMysqlDatabase(
		"root",
		"kakashulka",
		"127.0.0.1",
		"3306",
		"binance",
	)
	require.NoError(t, db.(*binanceDatabase).db.Ping())

}

func Test_parseInterfaceToTime(t *testing.T) {
	autotest.FunctionTesting(t, parseInterfaceToTime, testlog)

	now := time.Now().Truncate(time.Second)
	testTable := []struct {
		input  interface{}
		output time.Time
	}{
		{now.UnixNano() / int64(time.Millisecond), now},
		{"1499040000000", time.Unix(0, 0)},
		{1499040000000, time.Date(2017, 7, 3, 3, 0, 0, 0, time.Local)},
		{1598703162000, time.Date(2020, 8, 29, 15, 12, 42, 0, time.Local)},
	}
	for _, testCase := range testTable {
		require.True(t, testCase.output.Equal(parseInterfaceToTime(testlog, testCase.input)),
			fmt.Sprintf("input %v, output %v\ninput %v, output %v", testCase.input, testCase.output.UnixNano()/int64(time.Millisecond),
				parseInterfaceToTime(testlog, testCase.input), testCase.output))
	}
	testlog.logs = []string{}
}

func Test_parseInterfaceToInt64(t *testing.T) {
	autotest.FunctionTesting(t, parseInterfaceToInt64, testlog)

	testTable := []struct {
		input  interface{}
		output int64
	}{
		{1, 1},
		{1.0, 1},
		{"1", 0},
		{2.5, 0},
		{"2", 0},
	}
	for _, testCase := range testTable {
		require.Equal(t, testCase.output, parseInterfaceToInt64(testlog, testCase.input), fmt.Sprintf("input %v", testCase.input))
	}
	testlog.logs = []string{}
}

func Test_parseInterfaceToFloat64(t *testing.T) {
	autotest.FunctionTesting(t, parseInterfaceToFloat64, testlog)

	testTable := []struct {
		input  interface{}
		output float64
	}{
		{"1.0", 1},
		{"21354.0", 21354},
		{2.5, 0},
		{"2", 2},
		{"2,2", 0},
	}
	for _, testCase := range testTable {
		require.Equal(t, testCase.output, parseInterfaceToFloat64(testlog, testCase.input), fmt.Sprintf("input %v", testCase.input))
	}
	testlog.logs = []string{}
}

func Test_parseInterfaceToInt(t *testing.T) {
	autotest.FunctionTesting(t, parseInterfaceToInt, testlog)

	testTable := []struct {
		input  interface{}
		output int
	}{
		{1, 1},
		{"21354", 0},
		{2.5, 0},
		{2.0, 2},
	}
	for _, testCase := range testTable {
		require.Equal(t, testCase.output, parseInterfaceToInt(testlog, testCase.input), fmt.Sprintf("input %v", testCase.input))
	}
	testlog.logs = []string{}
}

func Test_getTimeIntervalDuration(t *testing.T) {
	autotest.FunctionTesting(t, getTimeIntervalDuration, testlog)

	require.Equal(t, time.Minute, getTimeIntervalDuration(testlog, "1m"))
	require.Equal(t, 5*time.Minute, getTimeIntervalDuration(testlog, "5m"))
	require.Equal(t, 15*time.Minute, getTimeIntervalDuration(testlog, "15m"))
	require.Equal(t, 30*time.Minute, getTimeIntervalDuration(testlog, "30m"))
	require.Equal(t, 1*time.Hour, getTimeIntervalDuration(testlog, "1h"))
	require.Equal(t, 2*time.Hour, getTimeIntervalDuration(testlog, "2h"))
	require.Equal(t, 4*time.Hour, getTimeIntervalDuration(testlog, "4h"))
	require.Equal(t, 6*time.Hour, getTimeIntervalDuration(testlog, "6h"))
	require.Equal(t, 8*time.Hour, getTimeIntervalDuration(testlog, "8h"))
	require.Equal(t, 12*time.Hour, getTimeIntervalDuration(testlog, "12h"))
	require.Equal(t, 24*time.Hour, getTimeIntervalDuration(testlog, "1d"))
	require.Equal(t, 3*24*time.Hour, getTimeIntervalDuration(testlog, "3d"))
	require.Equal(t, 1*7*24*time.Hour, getTimeIntervalDuration(testlog, "1w"))
	require.Equal(t, 30*24*time.Hour, getTimeIntervalDuration(testlog, "1M"))

	require.Equal(t, time.Duration(0), getTimeIntervalDuration(testlog, "test"))
}

func Test_newBinanceSite(t *testing.T) {
	autotest.FunctionTesting(t, newBinanceSite)

	ds := newBinanceSite()
	require.NotNil(t, ds.client)
	require.Equal(t, "https://api.binance.com/", ds.addressApi)
	require.Equal(t, "api/v3/klines", ds.queryCandlestick)
}

//go:linkname t_binanceSite_tryQueryCandlestickRange github.com/LevZhurov/go-binance-api.(*binanceSite).tryQueryCandlestickRange
func t_binanceSite_tryQueryCandlestickRange(bs *binanceSite, symbol string, interval TimeIntervals, startTime, endTime time.Time, limit int) (list []*Candlestick, usedWeight, retryAfter string)
func Test_binanceSite_tryQueryCandlestickRange(t *testing.T) {
	autotest.FunctionTesting(t, t_binanceSite_tryQueryCandlestickRange)

	log.SetOutput(os.Stdout)
	defer log.SetOutput(ioutil.Discard)

	location, _ := time.LoadLocation("UTC")
	now := time.Date(2021, 9, 29, 0, 0, 0, 0, location)

	testTable := []struct {
		name               string
		handler            http.HandlerFunc
		timeStart          time.Time
		timeEnd            time.Time
		expectedList       []*Candlestick
		expectedUsedWeight string
		expectedRetryAfter string
	}{
		{
			name: "без указания диапазона",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add("X-Mbx-Used-Weight", "test weight")
				w.Header().Add("Retry-After", "test retry")

				fmt.Fprintf(w, `[[1632614400000,"42670.63000000","43950.00000000","40750.00000000","43160.90000000","49879.99765000",1632700799999,"2124657079.27977650",1374145,"24762.74447000","1055225792.12858580","0"],`+
					`[1632700800000,"43160.90000000","44350.00000000","42098.00000000","42147.35000000","39776.84383000",1632787199999,"1728912299.80342430",1001487,"19307.85346000","839585982.39070810","0"],`+
					`[1632787200000,"42147.35000000","42787.38000000","41634.75000000","41639.74000000","13134.76775000",1632873599999,"555177776.67904690",314261,"6160.33812000","260407635.38921180","0"]]`)
			}),
			expectedList: []*Candlestick{
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -3),
					Open:                     42670.63,
					High:                     43950,
					Low:                      40750,
					Close:                    43160.9,
					Volume:                   49879.99765,
					CloseTime:                now.AddDate(0, 0, -2).Add(-time.Millisecond),
					QuoteAssetVolume:         2.1246570792797766e+09,
					NumberOfTrades:           1374145,
					TakerBuyBaseAssetVolume:  24762.74447,
					TakerBuyQuoteAssetVolume: 1.0552257921285858e+09,
					Ignore:                   0,
				},
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -2),
					Open:                     43160.9,
					High:                     44350,
					Low:                      42098,
					Close:                    42147.35,
					Volume:                   39776.84383,
					CloseTime:                now.AddDate(0, 0, -1).Add(-time.Millisecond),
					QuoteAssetVolume:         1.7289122998034244e+09,
					NumberOfTrades:           1001487,
					TakerBuyBaseAssetVolume:  19307.85346,
					TakerBuyQuoteAssetVolume: 8.395859823907081e+08,
					Ignore:                   0,
				},
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -1),
					Open:                     42147.35,
					High:                     42787.38,
					Low:                      41634.75,
					Close:                    41639.74,
					Volume:                   13134.76775,
					CloseTime:                now.Add(-time.Millisecond),
					QuoteAssetVolume:         5.551777766790469e+08,
					NumberOfTrades:           314261,
					TakerBuyBaseAssetVolume:  6160.33812,
					TakerBuyQuoteAssetVolume: 2.604076353892118e+08,
					Ignore:                   0,
				},
			},
			expectedUsedWeight: "test weight",
			expectedRetryAfter: "test retry",
		},
		{
			name:      "с указанием диапазона",
			timeStart: now.AddDate(0, 0, -4),
			timeEnd:   now,
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add("X-Mbx-Used-Weight", "test weight")
				w.Header().Add("Retry-After", "test retry")

				fmt.Fprintf(w, `[[1632528000000,"42810.58000000","42966.84000000","41646.28000000","42670.64000000","33594.57189000",1632614399999,"1428238748.20952580",949787,"16555.72950000","703947971.38327600","0"],`+
					`[1632614400000,"42670.63000000","43950.00000000","40750.00000000","43160.90000000","49879.99765000",1632700799999,"2124657079.27977650",1374145,"24762.74447000","1055225792.12858580","0"],`+
					`[1632700800000,"43160.90000000","44350.00000000","42098.00000000","42147.35000000","39776.84383000",1632787199999,"1728912299.80342430",1001487,"19307.85346000","839585982.39070810","0"]]`)
			}),
			expectedList: []*Candlestick{
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -4),
					Open:                     42810.58,
					High:                     42966.84,
					Low:                      41646.28,
					Close:                    42670.64,
					Volume:                   33594.57189,
					CloseTime:                now.AddDate(0, 0, -3).Add(-time.Millisecond),
					QuoteAssetVolume:         1.4282387482095258e+09,
					NumberOfTrades:           949787,
					TakerBuyBaseAssetVolume:  16555.7295,
					TakerBuyQuoteAssetVolume: 7.03947971383276e+08,
					Ignore:                   0,
				},
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -3),
					Open:                     42670.63,
					High:                     43950,
					Low:                      40750,
					Close:                    43160.9,
					Volume:                   49879.99765,
					CloseTime:                now.AddDate(0, 0, -2).Add(-time.Millisecond),
					QuoteAssetVolume:         2.1246570792797766e+09,
					NumberOfTrades:           1374145,
					TakerBuyBaseAssetVolume:  24762.74447,
					TakerBuyQuoteAssetVolume: 1.0552257921285858e+09,
					Ignore:                   0,
				},
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -2),
					Open:                     43160.9,
					High:                     44350,
					Low:                      42098,
					Close:                    42147.35,
					Volume:                   39776.84383,
					CloseTime:                now.AddDate(0, 0, -1).Add(-time.Millisecond),
					QuoteAssetVolume:         1.7289122998034244e+09,
					NumberOfTrades:           1001487,
					TakerBuyBaseAssetVolume:  19307.85346,
					TakerBuyQuoteAssetVolume: 8.395859823907081e+08,
					Ignore:                   0,
				},
			},
			expectedUsedWeight: "test weight",
			expectedRetryAfter: "test retry",
		},
		{
			name:      "с указанием начала диапазона",
			timeStart: now.AddDate(0, 0, -4),
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add("X-Mbx-Used-Weight", "test weight")
				w.Header().Add("Retry-After", "test retry")

				fmt.Fprintf(w, `[[1632528000000,"42810.58000000","42966.84000000","41646.28000000","42670.64000000","33594.57189000",1632614399999,"1428238748.20952580",949787,"16555.72950000","703947971.38327600","0"],`+
					`[1632614400000,"42670.63000000","43950.00000000","40750.00000000","43160.90000000","49879.99765000",1632700799999,"2124657079.27977650",1374145,"24762.74447000","1055225792.12858580","0"],`+
					`[1632700800000,"43160.90000000","44350.00000000","42098.00000000","42147.35000000","39776.84383000",1632787199999,"1728912299.80342430",1001487,"19307.85346000","839585982.39070810","0"]]`)
			}),
			expectedList: []*Candlestick{
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -4),
					Open:                     42810.58,
					High:                     42966.84,
					Low:                      41646.28,
					Close:                    42670.64,
					Volume:                   33594.57189,
					CloseTime:                now.AddDate(0, 0, -3).Add(-time.Millisecond),
					QuoteAssetVolume:         1.4282387482095258e+09,
					NumberOfTrades:           949787,
					TakerBuyBaseAssetVolume:  16555.7295,
					TakerBuyQuoteAssetVolume: 7.03947971383276e+08,
					Ignore:                   0,
				},
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -3),
					Open:                     42670.63,
					High:                     43950,
					Low:                      40750,
					Close:                    43160.9,
					Volume:                   49879.99765,
					CloseTime:                now.AddDate(0, 0, -2).Add(-time.Millisecond),
					QuoteAssetVolume:         2.1246570792797766e+09,
					NumberOfTrades:           1374145,
					TakerBuyBaseAssetVolume:  24762.74447,
					TakerBuyQuoteAssetVolume: 1.0552257921285858e+09,
					Ignore:                   0,
				},
				&Candlestick{
					OpenTime:                 now.AddDate(0, 0, -2),
					Open:                     43160.9,
					High:                     44350,
					Low:                      42098,
					Close:                    42147.35,
					Volume:                   39776.84383,
					CloseTime:                now.AddDate(0, 0, -1).Add(-time.Millisecond),
					QuoteAssetVolume:         1.7289122998034244e+09,
					NumberOfTrades:           1001487,
					TakerBuyBaseAssetVolume:  19307.85346,
					TakerBuyQuoteAssetVolume: 8.395859823907081e+08,
					Ignore:                   0,
				},
			},
			expectedUsedWeight: "test weight",
			expectedRetryAfter: "test retry",
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			binanceServer := httptest.NewTLSServer(testCase.handler)
			defer binanceServer.Close()

			bs := &binanceSite{
				client:           binanceServer.Client(),
				addressApi:       binanceServer.URL, //https://api.binance.com/
				queryCandlestick: "",                //api/v3/klines
			}
			list, uw, ra := bs.tryQueryCandlestickRange(
				"BTCUSDT",
				TI_1d,
				testCase.timeStart,
				testCase.timeEnd,
				3,
			)

			for i, c := range list {
				require.True(t, testCase.expectedList[i].OpenTime.Equal(c.OpenTime),
					fmt.Sprintf("candle: #%v, expected: %v, actual: %v", i, testCase.expectedList[i].OpenTime, c.OpenTime))
				require.True(t, testCase.expectedList[i].CloseTime.Equal(c.CloseTime),
					fmt.Sprintf("candle: #%v, expected: %v, actual: %v", i, testCase.expectedList[i].CloseTime, c.CloseTime))
				require.Equal(t, testCase.expectedList[i].Close, c.Close, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].High, c.High, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].Ignore, c.Ignore, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].Low, c.Low, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].NumberOfTrades, c.NumberOfTrades, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].Open, c.Open, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].QuoteAssetVolume, c.QuoteAssetVolume, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].TakerBuyBaseAssetVolume, c.TakerBuyBaseAssetVolume, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].TakerBuyQuoteAssetVolume, c.TakerBuyQuoteAssetVolume, fmt.Sprintf("candle #%v", i))
				require.Equal(t, testCase.expectedList[i].Volume, c.Volume, fmt.Sprintf("candle #%v", i))
			}
			require.Equal(t, testCase.expectedUsedWeight, uw)
			require.Equal(t, testCase.expectedRetryAfter, ra)
		})
	}
}

func Test_parseCandlestick(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	autotest.FunctionTesting(t, parseCandlestick)

	require.Equal(t, &Candlestick{
		OpenTime:                 time.Unix(0, 1499040000000*int64(time.Millisecond)),
		Open:                     0.0163479,
		High:                     0.8,
		Low:                      0.015758,
		Close:                    0.015771,
		Volume:                   148976.11427815,
		CloseTime:                time.Unix(0, 1499644799999*int64(time.Millisecond)),
		QuoteAssetVolume:         2434.19055334,
		NumberOfTrades:           308,
		TakerBuyBaseAssetVolume:  1756.87402397,
		TakerBuyQuoteAssetVolume: 28.46694368,
		Ignore:                   17928899.62484339,
	}, parseCandlestick([]interface{}{
		1499040000000,       // Open time
		"0.01634790",        // Open
		"0.80000000",        // High
		"0.01575800",        // Low
		"0.01577100",        // Close
		"148976.11427815",   // Volume
		1499644799999,       // Close time
		"2434.19055334",     // Quote asset volume
		308,                 // Number of trades
		"1756.87402397",     // Taker buy base asset volume
		"28.46694368",       // Taker buy quote asset volume
		"17928899.62484339", // Ignore.
	}))
}
