package storage

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Makoto2024/BinanceTrader/BinanceAPI/klines"
)

var (
	KLineCSVHeader = []string{
		"OpenTime",
		"CloseTime",
		"OpenPrice",
		"ClosePrice",
		"HighPrice",
		"LowPrice",
		"Volume",
		"QuoteAssetVolume",
		"TradeNum",
	}
)

func timeToCSVRepr(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}

func timeFromCSVRepr(repr string) (time.Time, error) {
	ms, err := strconv.ParseInt(repr, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parseInt: %w", err)
	}
	return time.UnixMilli(ms), nil
}

func floatToCSVRepr(f float64) string {
	return strconv.FormatFloat(f, 'e', 6, 64)
}

func floatFromCSVRepr(repr string) (float64, error) {
	f, err := strconv.ParseFloat(repr, 64)
	if err != nil {
		return 0, fmt.Errorf("parseFloat: %w", err)
	}
	return f, nil
}

func KLineToCSVRecord(l *klines.KLine) []string {
	return []string{
		timeToCSVRepr(l.OpenTime),
		timeToCSVRepr(l.CloseTime),
		floatToCSVRepr(l.OpenPrice),
		floatToCSVRepr(l.ClosePrice),
		floatToCSVRepr(l.HighPrice),
		floatToCSVRepr(l.LowPrice),
		floatToCSVRepr(l.Volume),
		floatToCSVRepr(l.QuoteAssetVolume),
		floatToCSVRepr(l.TradeNum),
	}
}

func KLineFromCSVRecord(record []string, dst *klines.KLine) error {
	if len(record) != len(KLineCSVHeader) {
		return fmt.Errorf("expect %d column but get %d", len(KLineCSVHeader), len(record))
	}
	var err error
	if dst.OpenTime, err = timeFromCSVRepr(record[0]); err != nil {
		return fmt.Errorf("parse open time column %q: %w", record[0], err)
	}
	if dst.CloseTime, err = timeFromCSVRepr(record[1]); err != nil {
		return fmt.Errorf("parse close time column %q: %w", record[1], err)
	}
	if dst.OpenPrice, err = floatFromCSVRepr(record[2]); err != nil {
		return fmt.Errorf("parse open price column %q: %w", record[2], err)
	}
	if dst.ClosePrice, err = floatFromCSVRepr(record[3]); err != nil {
		return fmt.Errorf("parse close price column %q: %w", record[3], err)
	}
	if dst.HighPrice, err = floatFromCSVRepr(record[4]); err != nil {
		return fmt.Errorf("parse high price column %q: %w", record[4], err)
	}
	if dst.LowPrice, err = floatFromCSVRepr(record[5]); err != nil {
		return fmt.Errorf("parse low price column %q: %w", record[5], err)
	}
	if dst.Volume, err = floatFromCSVRepr(record[6]); err != nil {
		return fmt.Errorf("parse volumne column %q: %w", record[6], err)
	}
	if dst.QuoteAssetVolume, err = floatFromCSVRepr(record[7]); err != nil {
		return fmt.Errorf("parse quote asset volume column %q: %w", record[7], err)
	}
	if dst.TradeNum, err = floatFromCSVRepr(record[8]); err != nil {
		return fmt.Errorf("parse trade num column %q: %w", record[8], err)
	}
	return nil
}
