package request

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
	// Echo
	echo "github.com/labstack/echo"
	// Custom package
	hdb "dems-api-server/controllers/query"
	anony "dems-api-server/controllers/anonymous"
)

const (
	// Data unit
	DU_KB = 1024
	DU_MB = DU_KB * 1024
	DU_GM = DU_MB * 1024
)

// Response structrue
type ResponseMessage struct {
	Result 	bool 			`json:"result" xml:"result"`
	Message []string 	`json:"message" xml:"message"`
}
type ResponseList struct {
	Result 	bool												`json:"result" xml:"result"`
	Message map[string](map[string]int) `json:"message" xml:"message"`
}
type ResponseRequestInfo struct {
	Result 	bool							`json:"result" xml:"result"`
	Message map[string]string	`json:"message" xml:"message"`
}
// Database interface
type ConnectionDB struct {
	db *sql.DB
	syntax string
	// Data size
	totalSize uint64
	blockSize uint64
}

func RequestList(ctx echo.Context) error {
	// Get workspace
	workspace, err := os.Getwd()
	if e := catchError(ctx, err); e != nil {
		return e
	}
	// Set directory path
	dirPath := path.Join(workspace, "./resources/processed")
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		printLog("error", err.Error())
		// Create directory with optional files
		err := os.MkdirAll(dirPath, 0644)
		if e:= catchError(ctx, err); e != nil {
			return e
		}
	}
	// Open directory with optional files
	dir, err := os.Open(dirPath)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	defer dir.Close()
	// Get directory info
	info, err := dir.Readdir(-1)
	if e := catchError(ctx, err); e != nil {
		return e
	}

	// Create obj to output
	accessObj := make(map[string](map[string]int), len(info))
	for _, file := range info {
		accessObj[file.Name()] = map[string]int {
			"attempt": 0,
			"success": 0,
			"failed": 0,
		}
	}
	// Lookup log file to know export history
	logFilePath := path.Join(workspace, "./resources/logs/access.log")
	if e := catchError(ctx, err); e != nil {
		return e
	}
	logFile, err := os.Open(logFilePath)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	defer logFile.Close()
	// Get export history data for each request
	reader := bufio.NewReader(logFile)
	for {
		line, isPrefix, err := reader.ReadLine()
		if isPrefix || err != nil {
			break
		}

		split := strings.Split(string(line), " ")
		switch split[1] {
			case "[Attempt]":
				accessObj[split[2]]["attempt"] += 1
			case "[Success]":
				accessObj[split[2]]["success"] += 1
			case "[Failed]":
				accessObj[split[2]]["failed"] += 1
		}
	}

	// Create response format
	message := &ResponseList{
		Result: true,
		Message: accessObj,
	}
	// Return
	return ctx.JSON(http.StatusOK, message)
}

func ExportRequest(ctx echo.Context) error {
	var err error
	requestID := ctx.Param("requestID")
	// Create database interface
	conn := new(ConnectionDB)
	conn.db, err = hdb.CreateConnection(requestID)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	defer conn.db.Close()
	// Set block size
	conn.blockSize = 100000

	// Get options
	options, err := hdb.GetOptions(requestID)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	// Create query syntax
	conn.syntax, err = hdb.CreateQuerySyntax(options)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	// Outputs the total number of query result
	conn.totalSize, err = hdb.GetDataSize(conn.db, conn.syntax)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	writeLog("access", "[Attempt] " + requestID)

	// Calculate the number of split queries basesd on the specified blocksize
	nProc := conn.totalSize / conn.blockSize
	if conn.totalSize % conn.blockSize > 0 {
		nProc += 1
	}
	
	// Create header to used in csv file
	header, err := hdb.GetDataColumns(conn.db, conn.syntax)
	if e := catchError(ctx, err); e != nil {
		return e
	}

	// Create channel(queue)
	rawDataQueue := make(chan []string, DU_MB * 256)
	pcdDataQueue := make(chan []string, DU_MB * 256)
	nProcQuery := make(chan bool, int(nProc))
	nProcAnony := make(chan bool, int(nProc))
	// stateQuery := make(chan bool)
	quitProc := make(chan bool)

	// Excute query
	_, err = hdb.ExecuteQuery(conn.db, conn.syntax, conn.blockSize, nProc, rawDataQueue, nProcQuery)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	// Process anonymization
	anony.Anonymization(requestID, nProc, header, rawDataQueue, pcdDataQueue, nProcAnony)
	// Save data
	go anony.SaveData(ctx.Response(), header, pcdDataQueue, quitProc)

	// 채널에 데이터 유무 확인 후, 채널 종료 처리 및 루프 종료 처리
	completedQuery := 0
	completedAnony := 0
	ProcLoop:
	for {
		select {
			case <-nProcQuery:
				completedQuery++
				if uint64(completedQuery) >= nProc {
					close(rawDataQueue)
				}
			case <-nProcAnony:
				completedAnony++
				if uint64(completedAnony) >= nProc {
					close(pcdDataQueue)
				}
			case <-quitProc:
				break ProcLoop
		}
	}
	
	printLog("debug", "Exported data")
	writeLog("access", "[Success] " + requestID)

	message := &ResponseMessage{
		Result: true,
		Message: []string{""},
	}
	conn = nil
	return ctx.JSON(http.StatusOK, message)
}

func RequestInfo(ctx echo.Context) error {
	requestID := ctx.QueryParam("id")
	// Check file path existance
	options, err := hdb.GetOptions(requestID)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	// Create query syntax
	syntax, err := hdb.CreateQuerySyntax(options)
	if e := catchError(ctx, err); e != nil {
		return e
	}
	
	// Create info object (string)
	rawConn := options["conn"].(map[string]interface{})
	var buffer bytes.Buffer
	buffer.WriteString(`{"endpoint":"`)
	buffer.WriteString(rawConn["host"].(string))
	buffer.WriteString(`:`)
	buffer.WriteString(rawConn["port"].(string))
	buffer.WriteString(`","database":"`)
	buffer.WriteString(rawConn["database"].(string))
	buffer.WriteString(`","table":"`)
	buffer.WriteString(rawConn["table"].(string))
	buffer.WriteString(`","syntax":"`)
	buffer.WriteString(syntax)
	buffer.WriteString(`"}`)
	strConn := buffer.String()
	// Convert json
	var connInfo map[string]string
	json.Unmarshal([]byte(strConn), &connInfo)
	// Create response message
	message := &ResponseRequestInfo{
		Result: true,
		Message: connInfo,
	}

	return ctx.JSON(http.StatusOK, message)
}

/* [Internal function] Outputs errors that occur during processing and terminates the process */
func catchError(ctx echo.Context, err error) error {
	if err != nil {
		// Create response format
		message := &ResponseMessage{
			Result: false,
			Message: []string{err.Error()},
		}
		// Return
		return ctx.JSON(http.StatusInternalServerError, message)
	} else {
		return nil
	}
}

/* [Internal function] Outputs log */
func printLog(pType string, message string) {
	// Create message
	var buffer bytes.Buffer
	switch pType {
		case "error":
			buffer.WriteString("\u001B[31m[ERROR] ")
		case "warning":
			buffer.WriteString("\u001B[33m[WARNING] ")
		case "success":
			buffer.WriteString("\u001B[32m[SUCCESS] ")
		case "debug":
			buffer.WriteString("\u001B[35m[DEBUG] ")
	}
	buffer.WriteString(message)
	buffer.WriteString("\u001B[0m")
	// Print message
	log.Print(buffer.String())
}

/* [Internal function] Write log for statistics processing */
func writeLog(logType string, message string) {
	// Get workspace
	workspace, err := os.Getwd()
	if err != nil {
		printLog("error", err.Error())
		return
	}
	
	dirPath := path.Join(workspace, "./resources/logs")
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, 0644)
		if err != nil {
			printLog("error", err.Error())
			return
		}
	}

	logFileName := logType + ".log"
	logFilePath := path.Join(dirPath, logFileName)
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		printLog("error", err.Error())
		return
	}
	defer file.Close()

	today := time.Now()
	file.WriteString(today.Format("2006-01-02T15:04:05 "))
	file.WriteString(message)
	file.WriteString("\n")
}