package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Makoto2024/BinanceTrader/BinanceAPI/common"
	"github.com/Makoto2024/BinanceTrader/BinanceAPI/klines"
	"github.com/Makoto2024/BinanceTrader/BinanceAPI/storage"
)

const (
	tickerSymbol = "SOLUSDT"
)

var (
	startTime        = time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime          = time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	interval         = common.ListKLinesInterval_1m
	intervalDuration = common.IntervalDuration(interval)

	ErrAllStoreFinished = errors.New("all stored")
	ErrNotConsecutive   = errors.New("not consecutive")
)

func formatTime(t time.Time) string {
	s := t.Format("2006-01-02 15:04")
	return fmt.Sprintf("%s (%v)", s, t.UnixMilli())
}

type KLineCollector struct {
	NextOpenTime time.Time
	LastOpenTime time.Time
	Interval     time.Duration
	LastKLine    *klines.KLine // For checking the consecutiveness.
}

// Collect all KLines within [from, to] inclusively.
func NewKLineCollector(from, to time.Time, dur time.Duration) *KLineCollector {
	if from.After(to) {
		to = from
	}
	// Find the biggest openTime within [from, to] such that it is "from + N * dur".
	expectKLineNum := (to.UnixMilli()-from.UnixMilli())/dur.Milliseconds() + 1
	return &KLineCollector{
		NextOpenTime: from,
		LastOpenTime: time.UnixMilli(from.UnixMilli() + (expectKLineNum-1)*dur.Milliseconds()),
		Interval:     dur,
		LastKLine:    nil,
	}
}

// Whether all expected KLines are retrieved.
func (c *KLineCollector) Finished() bool {
	return c.NextOpenTime.After(c.LastOpenTime)
}

// How many expected KLines left to be retrieved.
func (c *KLineCollector) KLinesLeft() uint64 {
	if c.Finished() {
		return 0
	}
	spanMS := c.LastOpenTime.Sub(c.NextOpenTime).Milliseconds()
	return uint64(spanMS/c.Interval.Milliseconds() + 1)
}

func (c *KLineCollector) isNextKLine(l *klines.KLine) bool {
	return l.OpenTime.UnixMilli() == c.NextOpenTime.UnixMilli()
}

// Checks if the passed-in KLine is the consecutive one and store it in the collector.
func (c *KLineCollector) StoreKLine(l *klines.KLine) error {
	if c.Finished() {
		return fmt.Errorf("all expected klines until %q are all stored: %w",
			formatTime(c.LastOpenTime), ErrAllStoreFinished)
	}
	if c.isNextKLine(l) {
		c.NextOpenTime = c.NextOpenTime.Add(c.Interval)
		c.LastKLine = l
		return nil
	}
	// Some ticker does not exists at the startTime; hence we skip to the first
	// existing KLine's openTime.
	if c.LastKLine == nil {
		c.NextOpenTime = l.OpenTime
		return c.StoreKLine(l)
	}
	return fmt.Errorf("expect open time %q but got %q: %w",
		formatTime(c.NextOpenTime), formatTime(l.OpenTime), ErrNotConsecutive)
}

func (c *KLineCollector) NextNextAPIStartTime() time.Time {
	nextAPIKLineNum := uint64(c.KLinesLeft())
	if nextAPIKLineNum > uint64(klines.ListKLinesMaxLimit) {
		nextAPIKLineNum = uint64(klines.ListKLinesMaxLimit)
	}
	return c.NextOpenTime.Add(time.Duration(nextAPIKLineNum) * c.Interval)
}

// Return the next startTime, endTime API parameters to retrieve the following KLines.
func (c *KLineCollector) NextAPIStartEndTime() (startTime, endTime time.Time) {
	return c.NextOpenTime, c.NextNextAPIStartTime().Add(-time.Millisecond)
}

func createNewCSV(path string) error {
	tmpPath := fmt.Sprintf("%s_tmp", path)
	fp, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	w := csv.NewWriter(fp)
	if err := w.Write(storage.KLineCSVHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	w.Flush()
	fp.Close()

	// Rename the file.
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename %q to %q: %w", tmpPath, path, err)
	}
	return nil
}

func continueCollectFromCSV(path string) (*KLineCollector, error) {
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat file: %w", err)
		}
		// Create file and start from the beggining.
		if err := createNewCSV(path); err != nil {
			return nil, fmt.Errorf("createNewCSV: %w", err)
		}
		return NewKLineCollector(startTime, endTime, intervalDuration), nil
	}

	// CSV exists, check if all stored data are valid (consecutive) and return a
	// collector with the continuing state.
	fp, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer fp.Close()
	r := csv.NewReader(fp)
	c := NewKLineCollector(startTime, endTime, intervalDuration)

	recordNum := -1 // Start from -1 because the first line is header hence not counted.
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read one CSV record: %w", err)
		}
		recordNum++
		if recordNum == 0 { // Skip parsing the first line, since it's the CSV header.
			continue
		}
		currKLine := &klines.KLine{}
		if err := storage.KLineFromCSVRecord(record, currKLine); err != nil {
			return nil, fmt.Errorf("KLineFromCSVRecord(%v): %w", record, err)
		}
		if err := c.StoreKLine(currKLine); err != nil {
			return nil, fmt.Errorf("StoreKLine(%+v): %w", currKLine, err)
		}
	}
	return c, nil
}

func main() {
	ctx := context.Background()
	csvPath := fmt.Sprintf("SOLUSDT_%s.csv", interval)

	// Get all 5m KLines.
	c, err := continueCollectFromCSV(csvPath)
	if err != nil {
		panic(err)
	}
	hasRecordInCSV := c.LastKLine != nil

	fp, err := os.OpenFile(csvPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer fp.Close()
	writer := csv.NewWriter(fp)
	defer writer.Flush()

	for !c.Finished() {
		apiStartTime, apiEndTime := c.NextAPIStartEndTime()
		fmt.Printf("API %q -> %q\n", formatTime(apiStartTime), formatTime(apiEndTime))
		lines, err := klines.ListKLines(ctx, klines.ListKLinesParam{
			TickerSymbol: tickerSymbol,
			Interval:     interval,
			StartTime:    apiStartTime,
			EndTime:      apiEndTime,
		})
		if err != nil {
			panic(err)
		}
		if len(lines) == 0 {
			if hasRecordInCSV {
				fmt.Printf("No new KLines starting from time %v", apiStartTime)
				break
			}
			// Because the ticker does not exist at this time, jump to the next
			// possible start time.
			c.NextOpenTime = c.NextNextAPIStartTime()
			fmt.Printf("No historical KLine between %q ~ %q, skip to %q",
				formatTime(apiStartTime), formatTime(apiEndTime), formatTime(c.NextOpenTime))
			continue
		}
	LOOP:
		// Update the collector's time and write to CSV.
		for idx := range lines {
			line := &lines[idx]
			for {
				var lineToWrite *klines.KLine
				err := c.StoreKLine(line)
				if err == nil {
					lineToWrite = line
				} else if errors.Is(err, ErrAllStoreFinished) {
					break LOOP
				} else if errors.Is(err, ErrNotConsecutive) {
					lineToWrite = &klines.KLine{}
					*lineToWrite = *c.LastKLine
					lineToWrite.OpenTime = c.NextOpenTime
					lineToWrite.CloseTime = c.NextOpenTime.Add(intervalDuration).
						Add(-time.Millisecond)
					fmt.Printf("Expect open time %q but got %q, fill with previous line instead\n",
						formatTime(lineToWrite.OpenTime), formatTime(line.OpenTime))
					if newErr := c.StoreKLine(lineToWrite); newErr != nil {
						panic(fmt.Errorf("recovering %v but failed: %w", err, newErr))
					}
				} else {
					panic(fmt.Errorf("StoreKLine(%+v): %w", line, err))
				}
				// Commit to CSV file.
				if err := writer.Write(storage.KLineToCSVRecord(lineToWrite)); err != nil {
					panic(fmt.Errorf("KLineToCSVRecord(%+v): %w", lineToWrite, err))
				}
				if lineToWrite == line {
					break
				}
			}
		}
	}
}
