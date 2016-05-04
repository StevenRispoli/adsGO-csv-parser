package main

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net/http"
	"strconv"
	"time"
)

var supportedCountries = map[string]struct{}{
	"AU": struct{}{},
	"CA": struct{}{},
	"GB": struct{}{},
	"US": struct{}{},
}

//Closing cancel channel signals reader or parser functions ending prematurely
var cancel chan struct{}
var done chan struct{}

type ip2locRec struct {
	ToIP        big.Int `json:"toIP"`
	CountryCode string  `json:"countryCode"`
	Region      string  `json:"region"`
	City        string  `json:"city"`
}

type appError struct {
	Error   error
	Message string
	Code    int
}

type appHandler func(http.ResponseWriter, *http.Request) *appError

func (fn appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if e := fn(w, r); e != nil {
		fmt.Printf("%v\n%s\n", e.Error, e.Message)
		http.Error(w, e.Message, e.Code)
	}
}

func main() {
	http.Handle("/", appHandler(ip2locInit))
	http.ListenAndServe(":3000", nil)
}

func ip2locInit(w http.ResponseWriter, r *http.Request) *appError {
	var recs []ip2locRec

	if b, rl, err := fetch("http://127.0.0.1:4000"); err != nil {
		return &appError{err, "Error fetching IP2Location data from IP2Location server", 404}
	} else {
		line := make(chan []string, 500000)
		chErr := make(chan error)

		cancel = make(chan struct{})
		done = make(chan struct{})

		//Read new lines as previous lines are being parsed
		go reader(b, rl, line, chErr)
		go parser(&recs, line, chErr)

		select {
		case e := <-chErr:
			return &appError{e, "Error preparing IP2Location data", 404}
		case <-done:
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("Recs-Length", strconv.Itoa(len(recs)))
		e := json.NewEncoder(w)
		for _, v := range recs {
			if err = e.Encode(&v); err != nil {
				return &appError{err, "Error marshalling IP2Location data", 404}
			}
		}
	}
	return nil
}

func fetch(url string) ([]byte, int64, error) {
	timeout := time.Duration(180 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	res, err := client.Get(url)
	if err != nil {
		return []byte{}, 0, err
	}

	b, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return []byte{}, 0, err
	}
	return b, res.ContentLength, nil
}

//Stop reader or parser if the other process was cancelled
func cancelled() bool {
	select {
	case <-cancel:
		return true
	default:
		return false
	}
}

func reader(body []byte, resLen int64, out chan<- []string, abort chan<- error) {
	defer close(out)

	zipPack, err := zip.NewReader(bytes.NewReader(body), resLen)
	if err != nil {
		abort <- err
		close(cancel)
		return
	}

	for _, f := range zipPack.File {
		if f.Name == "IPV6-COUNTRY-REGION-CITY.CSV" {
			rc, err := f.Open()
			if err != nil {
				abort <- err
				close(cancel)
				return
			}
			defer rc.Close()

			r := csv.NewReader(rc)
			//Records not required to have a certain number of fields
			r.FieldsPerRecord = -1

			for {
				if cancelled() {
					return
				}
				rec, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					abort <- err
					close(cancel)
					return
				}
				out <- rec
			}
		}
	}
}

func parser(ipRecs *[]ip2locRec, in <-chan []string, abort chan<- error) {
	for v := range in {
		if cancelled() {
			return
		}
		ipNum := big.NewInt(0)
		if _, ok := ipNum.SetString(v[1], 10); !ok {
			abort <- fmt.Errorf("Error with record: %v\n", v)
			close(cancel)
			return
		}
		if v[2] == "-" {
			continue
		}
		rec := ip2locRec{
			ToIP:        *ipNum,
			CountryCode: v[2],
		}
		if _, exists := supportedCountries[v[2]]; exists {
			rec.Region = v[4]
			rec.City = v[5]
		}
		*ipRecs = append(*ipRecs, rec)
	}
	close(done)
}
