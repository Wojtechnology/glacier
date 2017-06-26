package common

import "time"

func Now() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
}
