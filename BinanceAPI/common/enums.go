package common

import "time"

type ListKLinesInterval string

const (
	ListKLinesInterval_1m  ListKLinesInterval = "1m"  // 1 minute
	ListKLinesInterval_3m  ListKLinesInterval = "3m"  // 3 minutes
	ListKLinesInterval_5m  ListKLinesInterval = "5m"  // 5 minutes
	ListKLinesInterval_15m ListKLinesInterval = "15m" // 15 minutes
	ListKLinesInterval_30m ListKLinesInterval = "30m" // 30 minutes
	ListKLinesInterval_1h  ListKLinesInterval = "1h"  // 1 hour
	ListKLinesInterval_2h  ListKLinesInterval = "2h"  // 2 hours
	ListKLinesInterval_4h  ListKLinesInterval = "4h"  // 4 hours
	ListKLinesInterval_6h  ListKLinesInterval = "6h"  // 6 hours
	ListKLinesInterval_8h  ListKLinesInterval = "8h"  // 8 hours
	ListKLinesInterval_12h ListKLinesInterval = "12h" // 12 hours
	ListKLinesInterval_1d  ListKLinesInterval = "1d"  // 1 day
	ListKLinesInterval_3d  ListKLinesInterval = "3d"  // 3 days
	ListKLinesInterval_1w  ListKLinesInterval = "1w"  // 1 week
)

var listKLinesIntervalToDuration = map[ListKLinesInterval]time.Duration{
	ListKLinesInterval_1m:  time.Minute,
	ListKLinesInterval_3m:  3 * time.Minute,
	ListKLinesInterval_5m:  5 * time.Minute,
	ListKLinesInterval_15m: 15 * time.Minute,
	ListKLinesInterval_30m: 30 * time.Minute,
	ListKLinesInterval_1h:  time.Hour,
	ListKLinesInterval_2h:  2 * time.Hour,
	ListKLinesInterval_4h:  4 * time.Hour,
	ListKLinesInterval_6h:  6 * time.Hour,
	ListKLinesInterval_8h:  8 * time.Hour,
	ListKLinesInterval_12h: 12 * time.Hour,
	ListKLinesInterval_1d:  24 * time.Hour,
	ListKLinesInterval_3d:  3 * 24 * time.Hour,
	ListKLinesInterval_1w:  7 * 24 * time.Hour,
}

func IntervalDuration(i ListKLinesInterval) time.Duration {
	return listKLinesIntervalToDuration[i]
}
