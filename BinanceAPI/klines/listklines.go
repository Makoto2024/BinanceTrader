package klines

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/Makoto2024/BinanceTrader/BinanceAPI/common"
)

const (
	ListKLinesMaxLimit uint32 = 1500
)

type KLine struct {
	OpenTime         time.Time
	CloseTime        time.Time
	OpenPrice        float64
	ClosePrice       float64
	HighPrice        float64
	LowPrice         float64
	Volume           float64 // Number of BTC when referring to BTC/USDT.
	QuoteAssetVolume float64 // Number of USDT when referring to BTC/USDT.
	TradeNum         float64 // Number of trades.
}

// ListKLines API will return the KLines of the specified ticker in chronological order
// within [StartTime, EndTime] inclusively.
//
// If limit exceeds 1500 or if limit = 0, then the API returns the first 1500 KLines.
// Otherwise, the API will return the first "limit" number of KLines.
type ListKLinesParam struct {
	TickerSymbol string
	Interval     common.ListKLinesInterval
	StartTime    time.Time
	EndTime      time.Time
	Limit        uint32
}

func ListKLines(ctx context.Context, param ListKLinesParam) ([]KLine, error) {
	if param.StartTime.After(param.EndTime) {
		return nil, nil
	}
	if param.Limit == 0 || param.Limit >= ListKLinesMaxLimit {
		param.Limit = ListKLinesMaxLimit
	}
	return listKLineAPI(ctx, &param)
}

func listKLineAPI(ctx context.Context, param *ListKLinesParam) ([]KLine, error) {
	// Prepare request.
	const apiURL = common.RootAPIEndPoint + "/fapi/v1/klines"
	client := http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("new request url %q: %w", apiURL, err)
	}
	query := url.Values{}
	query.Add("symbol", param.TickerSymbol)
	query.Add("interval", string(param.Interval))
	query.Add("startTime", strconv.FormatInt(param.StartTime.UnixMilli(), 10))
	query.Add("endTime", strconv.FormatInt(param.EndTime.UnixMilli(), 10))
	query.Add("limit", strconv.FormatUint(uint64(param.Limit), 10))
	req.URL.RawQuery = query.Encode()

	// Execute HTTP request.
	rsp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get query %q: %w", req.URL.RawQuery, err)
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(rsp.Body)
		if err != nil {
			return nil, fmt.Errorf("http status code(%d) status(%q)", rsp.StatusCode, rsp.Status)
		}
		return nil, fmt.Errorf("http status code(%d) status(%q) body(%q)", rsp.StatusCode, rsp.Status, body)
	}

	return parseListKLinesRsp(ctx, rsp.Body)
}

func parseListKLinesRsp(ctx context.Context, body io.ReadCloser) ([]KLine, error) {
	decoder := json.NewDecoder(body)
	var entries []any
	if err := decoder.Decode(&entries); err != nil {
		return nil, fmt.Errorf("json decoder decode: %w", err)
	}

	lines := make([]KLine, len(entries))
	for idx, entry := range entries {
		entryArr, ok := entry.([]any)
		if !ok {
			return nil, fmt.Errorf("entry %d is not a JSON array", idx)
		}
		// Parse out each fields from the array.
		if err := parseListKLineRspOneEntry(ctx, entryArr, &lines[idx]); err != nil {
			return nil, fmt.Errorf("parseListKLineRspOneEntry (entry: %v): %w", entryArr, err)
		}
	}

	// Sort the KLines by open time.
	slices.SortFunc(lines, func(a, b KLine) int {
		return int(a.OpenTime.Sub(b.OpenTime).Milliseconds())
	})
	return lines, nil
}

func parseListKLineRspOneEntry(_ context.Context, entry []any, dst *KLine) error {
	const wantFieldNum = 9
	if len(entry) < wantFieldNum {
		return fmt.Errorf("only have %d entries (want %d)", len(entry), wantFieldNum)
	}

	// Open time.
	if v, ok := entry[0].(float64); !ok {
		return errors.New("expect float64 in open time field")
	} else {
		dst.OpenTime = time.UnixMilli(int64(v))
	}

	// Open price.
	if v, err := common.ParseFloat64FromAnyString(entry[1]); err != nil {
		return fmt.Errorf("parse open price field")
	} else {
		dst.OpenPrice = v
	}

	// High price.
	if v, err := common.ParseFloat64FromAnyString(entry[2]); err != nil {
		return fmt.Errorf("parse high price field")
	} else {
		dst.HighPrice = v
	}

	// Low price.
	if v, err := common.ParseFloat64FromAnyString(entry[3]); err != nil {
		return fmt.Errorf("parse low price field")
	} else {
		dst.LowPrice = v
	}

	// Close price.
	if v, err := common.ParseFloat64FromAnyString(entry[4]); err != nil {
		return fmt.Errorf("parse close price field")
	} else {
		dst.ClosePrice = v
	}

	// Volume.
	if v, err := common.ParseFloat64FromAnyString(entry[5]); err != nil {
		return fmt.Errorf("parse volume field")
	} else {
		dst.Volume = v
	}

	// Close time.
	if v, ok := entry[6].(float64); !ok {
		return errors.New("expect float64 in close time field")
	} else {
		dst.CloseTime = time.UnixMilli(int64(v))
	}

	// Quote asset volume.
	if v, err := common.ParseFloat64FromAnyString(entry[7]); err != nil {
		return fmt.Errorf("parse quote asset volume field")
	} else {
		dst.QuoteAssetVolume = v
	}

	// Trade num.
	if v, ok := entry[8].(float64); !ok {
		return errors.New("expect float64 in trade num field")
	} else {
		dst.TradeNum = v
	}
	return nil
}
