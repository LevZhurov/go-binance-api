package binance_api

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_QueryCandlestickList(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Minute)
	list := QueryCandlestickList(
		"SCUSDT",
		TI_1m,
		now.AddDate(0, 0, -1),
		now,
	)
	var prevOpenTime time.Time
	for i, candle := range list {
		if i != 0 {
			require.Equal(t, prevOpenTime.UnixNano()+int64(time.Minute), candle.OpenTime.UnixNano(),
				fmt.Sprintf("%v: %v, %v", i, prevOpenTime.Add(time.Minute), candle.OpenTime),
			)
		}
		prevOpenTime = candle.OpenTime
	}
	// t.Logf("\n%v\n%v\n%v\n%v",
	// 	now.AddDate(0, 0, -1),
	// 	list[0].OpenTime,
	// 	now,
	// 	list[len(list)-1].OpenTime,
	// )
	require.Equal(t, 1441, len(list))
	require.True(t, list[0].OpenTime.Equal(now.Add(-time.Hour*24)))
	require.True(t, list[len(list)-1].OpenTime.Equal(now))
}

func Test_queryCandlestickRange(t *testing.T) {
	//без указания диапазона
	list, uw, ra := queryCandlestickRange(
		"SCUSDT",
		TI_1m,
		time.Time{},
		time.Time{},
		3,
	)
	uwi, err := strconv.Atoi(uw)
	require.NoError(t, err)
	require.Less(t, 0, uwi)
	require.Greater(t, 10, uwi)
	require.Equal(t, "", ra)
	require.Equal(t, 3, len(list))
	require.NotNil(t, list[0])
	require.NotNil(t, list[1])
	require.NotNil(t, list[2])

	//с указанием диапазона
	list, uw, ra = queryCandlestickRange(
		"SCUSDT",
		TI_1m,
		time.Now().AddDate(0, 0, -1),
		time.Now(),
		1000,
	)
	uwi, err = strconv.Atoi(uw)
	require.NoError(t, err)
	require.Less(t, 0, uwi)
	require.Greater(t, 10, uwi)
	require.Equal(t, "", ra)
	require.Equal(t, 1000, len(list))

	//с указанием начала диапазона
	list, uw, ra = queryCandlestickRange(
		"SCUSDT",
		TI_1m,
		time.Now().AddDate(0, 0, -1),
		time.Time{},
		1000,
	)
	uwi, err = strconv.Atoi(uw)
	require.NoError(t, err)
	require.Less(t, 0, uwi)
	require.Equal(t, "", ra)
	require.Equal(t, 1000, len(list))
}

func Test_parseCandlestick(t *testing.T) {
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
