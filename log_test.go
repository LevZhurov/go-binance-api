package binance_api

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_newLogCollector(t *testing.T) {
	lc := newLogCollector()
	lc.logs = append(lc.logs, "")
}

func Test_LogCollector_Printf(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	var lc *logCollector
	lc.Printf("%v", 1)
	require.Nil(t, lc)

	lc = newLogCollector()
	lc.Printf("%v", 1)
	require.Equal(t, []string{"1"}, lc.logs)
}

func Test_LogCollector_Println(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	var lc *logCollector
	lc.Println(1)
	require.Nil(t, lc)

	lc = newLogCollector()
	lc.Println(1)
	require.Equal(t, []string{"1\n"}, lc.logs)
}
