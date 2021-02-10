package anonymous

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// define some error code
const (
	ErrorParameter = 2
	// execution format error
	ErrorOptionFile = 3
	// option file read error
	ErrorOptionFormat = 4  //  option file format error
	ErrorInputFormat  = 5  // input file format error
	ErrorOutput       = 6  // file write error
	ErrorInternal     = 10 // mapping function execution error
)

// AnoOption defines the specific anonymization option parameter format
type AnoOption struct {
	Fore       string `json:"fore,omitempty"`
	Aft        string `json:"aft,omitempty"`
	MaskChar   string `json:"maskChar,omitempty"`
	KeepLength string `json:"keepLength,omitempty"`
	Algorithm  string `json:"algorithm,omitempty"`
	Position   int    `json:"position,omitempty"`
	Unit       string `json:"unit,omitempty"`
	Key        string `json:"key,omitempty"`
	Digest     string `json:"digest,omitempty"`
	Lower      string `json:"lower,omitempty"`
	Upper      string `json:"upper,omitempty"`
	Bin        string `json:"bin,omitempty"`
	Linear     string `json:"linear,omitempty"`
}

// Option defines the field anonymization method parameter format
type Option struct {
	Method      string    `json:"method"`
	Options     AnoOption `json:"options"`
	Level       int       `json:"level"`
	Description string    `json:"description"`
}

func buildEncryptingFunc(options AnoOption) func(string) string {
	switch options.Algorithm {
	case "hmac":
		switch options.Digest {
		case "sha256":
			mac := hmac.New(sha256.New, []byte(options.Key))
			return func(inString string) string {
				mac.Write([]byte(inString))
				defer mac.Reset()
				return hex.EncodeToString(mac.Sum(nil))
			}
		case "md5":
			mac := hmac.New(md5.New, []byte(options.Key))
			return func(inString string) string {
				mac.Write([]byte(inString))
				defer mac.Reset()
				return hex.EncodeToString(mac.Sum(nil))
			}
		default:
			mac := hmac.New(sha256.New, []byte(options.Key))
			return func(inString string) string {
				mac.Write([]byte(inString))
				defer mac.Reset()
				return hex.EncodeToString(mac.Sum(nil))
			}
		}
	case "hash(sha256)":
		mac := sha256.New()
		return func(inString string) string {
			mac.Write([]byte(inString))
			defer mac.Reset()
			return hex.EncodeToString(mac.Sum(nil))
		}
	case "hash(md5)":
		mac := md5.New()
		return func(inString string) string {
			mac.Write([]byte(inString))
			defer mac.Reset()
			return hex.EncodeToString(mac.Sum(nil))
		}
	default:
		return func(inString string) string {
			return "unknown Encrypting algorithm"
		}
	}
}

func buildRoundingFunc(options AnoOption) func(string) string {
	/*position, err := strconv.ParseInt(options.Position, 10, 0)
	if err != nil {
		return func (inString string) string {
			return "position parameter error"
		}
	}*/
	position := int(options.Position)
	posPower := math.Pow(10, math.Abs(float64(position)))
	switch options.Algorithm {
	case "round":
		return func(inString string) string {
			if value, err := strconv.ParseFloat(inString, 64); err == nil {
				if position > 0 {
					return strconv.FormatFloat(math.Round(value*posPower)/posPower, 'f', position, 64)
				}
				return strconv.FormatFloat(math.Round(value/posPower)*posPower, 'f', 0, 64)
			}
			return "parseFloat error:" + inString
		}
	case "ceil":
		return func(inString string) string {
			if value, err := strconv.ParseFloat(inString, 64); err == nil {
				if position > 0 {
					return strconv.FormatFloat(math.Ceil(value*posPower)/posPower, 'f', position, 64)
				}
				return strconv.FormatFloat(math.Ceil(value/posPower)*posPower, 'f', 0, 64)
			}
			return "parseFloat error:" + inString

		}
	case "floor":
		return func(inString string) string {
			if value, err := strconv.ParseFloat(inString, 64); err == nil {
				if position > 0 {
					return strconv.FormatFloat(math.Floor(value*posPower)/posPower, 'f', position, 64)
				}
				return strconv.FormatFloat(math.Floor(value/posPower)*posPower, 'f', 0, 64)
			}
			return "parseFloat error:" + inString
		}
	default:
		return func(inString string) string {
			return "unknown Rounding algorithm"
		}
	}
}

func buildRangingFunc(options AnoOption) func(string) string {
	lowBound, err := strconv.ParseFloat(options.Lower, 64)
	if err != nil {
		return func(inString string) string {
			return "lower parameter error"
		}
	}
	upBound, err2 := strconv.ParseFloat(options.Upper, 64)
	if err2 != nil {
		return func(inString string) string {
			return "upper parameter error"
		}
	}
	binNumP, err3 := strconv.ParseInt(options.Bin, 10, 0)
	if err3 != nil {
		return func(inString string) string {
			return "bin parameter error"
		}
	}
	binNum := int(binNumP)
	boundary := []float64{}
	//boundary :=
	for i := 0; i < binNum; i++ {
		boundary = append(boundary, lowBound+((upBound-lowBound)/float64(binNum))*float64(i))
	}
	boundary = append(boundary, upBound)

	return func(inString string) string {
		if value, err := strconv.ParseFloat(inString, 64); err == nil {
			before := ""
			last := ""
			for _, bound := range boundary {
				if bound > value {
					return fmt.Sprint(before, " ~ ", bound)
				}
				before = fmt.Sprintf("%v", bound) //bound
				last = fmt.Sprintf("%v", bound)
			}
			return fmt.Sprint(last, " ~ ")
		}
		return "parseFloat error:" + inString
	}
}

func buildMaskingFunc(options AnoOption) func(string) string {
	//maskPattern = '(^.{{{startlen}}})(.*)(.{{{endlen}}}$)'
	fore, err := strconv.ParseInt(options.Fore, 10, 0)
	if err != nil {
		return func(inString string) string {
			return "fore parameter error"
		}
	}
	aft, err1 := strconv.ParseInt(options.Aft, 10, 0)
	if err1 != nil {
		return func(inString string) string {
			return "aft parameter error"
		}
	}
	maskChar := options.MaskChar
	keepLength, err2 := strconv.ParseBool(options.KeepLength)
	if err2 != nil {
		return func(inString string) string {
			return "keepLength parameter error"
		}
	}
	reString := fmt.Sprintf("(^.{%v})(.*)(.{%v}$)", fore, aft)
	re := regexp.MustCompile(reString)

	//reObject = re.compile(maskPattern.format(startlen=fore, endlen=aft))
	return func(inString string) string {
		if inString == "" {
			return ""
		}
		if len(inString) >= int(fore+aft) {
			resIndex := re.FindStringSubmatchIndex(inString)
			if keepLength {
				maskLen := resIndex[5] - resIndex[4]
				repeatNum := math.Ceil(float64(maskLen / len(maskChar)))
				mask := strings.Repeat(maskChar, int(repeatNum))
				return inString[resIndex[2]:resIndex[3]] + mask[0:maskLen] + inString[resIndex[6]:resIndex[7]]
			}
			return inString[resIndex[2]:resIndex[3]] + maskChar + inString[resIndex[6]:resIndex[7]]
		}
		return ""
	}
}

func procData(options map[string]Option, headerInfo []string, iChan <-chan []string, oChan chan<- []string, termChan chan<- bool) {
	// build processing functions
	funcList := [](func(string) string){}
	passAsIs := func(inString string) string {
		return inString
	}
	dropAll := func(inString string) string {
		return ""
	}
	//fmt.Println("Within procData...")
	//fmt.Println(options)
	for _, key := range headerInfo {
		if option, exists := options[key]; exists == true {
			switch option.Method {
			case "encryption":
				//fmt.Println(key, ":Encrypting")
				funcList = append(funcList, buildEncryptingFunc(option.Options))
			case "rounding":
				//fmt.Println(key, ":Rounding")
				funcList = append(funcList, buildRoundingFunc(option.Options))
			case "data_range":
				//fmt.Println(key, ":Ranging")
				funcList = append(funcList, buildRangingFunc(option.Options))
			case "blank_impute":
				//fmt.Println(key, ":Masking")
				funcList = append(funcList, buildMaskingFunc(option.Options))
			case "pii_reduction":
				//fmt.Println(key, ":Masking pii")
				funcList = append(funcList, buildMaskingFunc(option.Options))
			case "non":
				//fmt.Println(key, ":non")
				funcList = append(funcList, passAsIs)
			default:
				//fmt.Println(key, ": ... unknown drop")
				funcList = append(funcList, dropAll)

			}
		} else {
			//fmt.Printf("%s: no key found in options\n", key)
			funcList = append(funcList, passAsIs)
		}
	}
	// for each input from iChan
	// fmt.Println("Proc running...")

	cnt := 0
	for v, ok := <-iChan; ok; v, ok = <-iChan {
		// blocking
		// do some processing
		// and then send the result to oChan
		output := []string{}
		for i, value := range v {
			output = append(output, funcList[i](value))
		}
		//fmt.Print(output)
		oChan <- output
		cnt++
	}

	printLog("debug", "Routine(Anonymization) exit (DataCount:" + strconv.Itoa(cnt) + ")")
	termChan <- true
}

/* [Function] 비식별화 처리 */
func Anonymization(requestID string, nProc uint64, header []string, rawDataQueue <-chan []string, pcdDataQueue chan<- []string, nProcAnony chan<- bool) {
	// 비식별화 정보를 가진 파일 경로 생성
	workspace, err := os.Getwd()
	if err != nil {
		printLog("error", err.Error())
		return
	}
	filePath := path.Join(workspace, "./resources/processed/", requestID, "options.json")
	// 옵션 파일 데이터 읽어오기
	optionContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Print(err)
	}
	// 데이터 변환(buffer -> json)
	var options map[string]Option
	err = json.Unmarshal(optionContent, &options)
	if err != nil {
		fmt.Print(err)
	}

	// 비식별화 처리
	for i := uint64(0); i < nProc; i++ {
		go procData(options, header, rawDataQueue, pcdDataQueue, nProcAnony)
	}
}

/* [Function] 비식별화된 데이터 저장 */
func SaveData(res http.ResponseWriter, header []string, pcdDataQueue <-chan []string, quitProc chan<- bool) {
	// response header 설정
	res.Header().Set("Connection", "Keep-Alive")
	res.Header().Set("Transfer-Encoding", "chunked")
	res.Header().Set("X-Content-Type-Options", "nosniff")
	// stream file setting
	res.Header().Set("Content-Disposition", "attachment;filename=exportData.csv")
	res.Header().Set("Content-Type", "application/octet-stream")
	// Write
	count := 0
	numFields := len(header)
	var buf bytes.Buffer
	for i, v := range(header) {
		if strings.ContainsAny(v, ", ") {
			buf.Write([]byte("\"" + v + "\""))
		} else {
			buf.Write([]byte(v))
		}
		if i < numFields -1 {
			buf.Write([]byte(", "))
		}
	}
	buf.Write([]byte("\r\n"))
	res.Write(buf.Bytes())
	buf.Reset()
	/* SaveLoop: */
	for x, ok := <-pcdDataQueue; ok; x, ok = <-pcdDataQueue {
		for i, v := range(x) {
			if strings.ContainsAny(v, ", ") {
				buf.Write([]byte("\"" + v + "\""))
			} else {
				buf.Write([]byte(v))
			}
			if i < numFields -1 {
				buf.Write([]byte(", "))
			}
		}
		buf.Write([]byte("\r\n"))
		res.Write(buf.Bytes())
		buf.Reset()
		count++
	}
	fmt.Println("SaveLoop writes total", count, "lines")
	quitProc <- true
}

/* [Internal function] Print log */
func printLog(logType string, message string) {
	var buf bytes.Buffer
	// matching by type
	switch logType {
		case "notice":	// Color is GREEN
			buf.WriteString("\x1b[32m[NOTICE] ")
			buf.WriteString(message)
			buf.WriteString("\x1b[0m")
		case "debug":			// Color is MAGENTA
			buf.WriteString("\x1b[35m[DEBUG] ")
			buf.WriteString(message)
			buf.WriteString("\x1b[0m")
		case "error":		// Color is RED
			buf.WriteString("\x1b[31m[ERROR] ")
			buf.WriteString(message)
			buf.WriteString("\x1b[0m")
		default:				// Color is DEFAULT (WHITE)
			buf.WriteString("\x1b[0m[ERROR] ")
			buf.WriteString(message)
			buf.WriteString("\x1b[0m")
	}
	// Print
	log.Print(buf.String())
}

/* [Internal function] Catch error */
func catchError(err error) {
	if err != nil {
		printLog("error", err.Error())
		panic(err)
	}
}