package main

import (
	"bufio"
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

// When continuing from last download, the last CSV record might not be
// a multiple of the specified inteval. For example, when downloading
// 1h KLines, the last download might stop at 23:30, which should be 24:00 instead.
func abandonLastCSVRecord(csvPath string) error {
	// Open the file for reading and writing.  We need read to count lines, and write to truncate.
	file, err := os.OpenFile(csvPath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var lastLine string
	var totalBytes int64

	// Iterate through the file to count offset of the beginning of the last line.
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			totalBytes += int64(len(line))
			lastLine = line
		}
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("ReadString('\n'): %w", err)
		}
	}

	bytesBeforeLastLine := totalBytes - int64(len(lastLine))
	if bytesBeforeLastLine == 0 {
		return nil // Empty file, nothing to do.
	}

	// Truncate the file to the offset of the second to last line.
	if err = file.Truncate(bytesBeforeLastLine); err != nil {
		return fmt.Errorf("Truncate(%d): %w", bytesBeforeLastLine, err)
	}
	return nil
}

func continueCollectFromCSV(
	startTime, endTime time.Time,
	interval common.ListKLinesInterval, path string,
) (*KLineCollector, error) {
	intervalDuration := common.IntervalDuration(interval)
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
	if err := abandonLastCSVRecord(path); err != nil {
		return nil, fmt.Errorf("abandonLastCSVRecord: %w", err)
	}

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

func downloadOneTimeFrame(
	ctx context.Context,
	startTime, endTime time.Time,
	interval common.ListKLinesInterval,
) error {
	intervalDuration := common.IntervalDuration(interval)
	csvPath := fmt.Sprintf("../../price_data/%s/%s_%s.csv",
		tickerSymbol, tickerSymbol, interval)

	// Get all 5m KLines.
	c, err := continueCollectFromCSV(startTime, endTime, interval, csvPath)
	if err != nil {
		return fmt.Errorf("continueCollectFromCSV(%q): %w", csvPath, err)
	}
	hasRecordInCSV := c.LastKLine != nil

	fp, err := os.OpenFile(csvPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile(%q): %w", csvPath, err)
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
			return fmt.Errorf("ListKLines: %w", err)
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
						return fmt.Errorf("recovering %v but failed: %w", err, newErr)
					}
				} else {
					return fmt.Errorf("StoreKLine(%+v): %w", line, err)
				}
				// Commit to CSV file.
				if err := writer.Write(storage.KLineToCSVRecord(lineToWrite)); err != nil {
					return fmt.Errorf("KLineToCSVRecord(%+v): %w", lineToWrite, err)
				}
				if lineToWrite == line {
					break
				}
			}
		}
	}
	return nil
}

func main() {
	ctx := context.Background()

	startTime := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, 2, 10, 0, 0, 0, 0, time.UTC)

	for _, interval := range []common.ListKLinesInterval{
		common.ListKLinesInterval_5m,
		common.ListKLinesInterval_15m,
		common.ListKLinesInterval_1h,
		common.ListKLinesInterval_4h,
		common.ListKLinesInterval_12h,
		common.ListKLinesInterval_1d,
	} {
		fmt.Printf("\n\nDownloading time frame %s\n", interval)
		if err := downloadOneTimeFrame(ctx, startTime, endTime, interval); err != nil {
			panic(fmt.Errorf("downloadOneTimeFrame(%v) failed with err %v", interval, err))
		}
	}
}
