package query

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	_ "fmt"
	"log"
	"math/big"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	// Driver
	"github.com/SAP/go-hdb/driver"
)

/* [Function] Create db object (using connector) */
func CreateConnection_old(host string, port string, user string, pwd string) (*sql.DB, error) {
	// Create dsn
	dsn := "hdb://" + user + ":" + pwd + "@" + host + ":" + port
	// Create connector
	connector, err := driver.NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	// Set connectior option
	connector.SetFetchSize(512)
	// Create db object
	db := sql.OpenDB(connector)
	// Test connection
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	// Return
	return db, nil
}

/* [Function] Create db object (using connector) */
func CreateConnection(requestID string) (*sql.DB, error) {
	// 데이터베이스 연결 정보 및 반출 처리 옵션 읽어오기
	options, err := GetOptions(requestID)
	if err != nil {
		return nil, err
	}
	// 데이터베이스 연결 정보 추출
	connInfo := options["conn"].(map[string]interface{})

	// DSN 생성
	dsn := "hdb://" + connInfo["user"].(string) + ":" + connInfo["pwd"].(string) + "@" + connInfo["host"].(string) + ":" + connInfo["port"].(string)
	// 커넥터 생성
	connector, err := driver.NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	// 커넥터 옵션 설정
	connector.SetFetchSize(512)
	// 데이터베이스 객체 생성
	db := sql.OpenDB(connector)
	// 연결 테스트
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	// Return
	return db, nil
}

/* [Function] Query */
func ExecuteQuery(db *sql.DB, syntax string, blockSize uint64, nProc uint64, dataQueue chan<- []string, nProcQuery chan<- bool) (bool, error) {
	// 멀티 프로세싱을 위해 Max proc 값 설정
	runtime.GOMAXPROCS(runtime.NumCPU() * 4)

	// 분할 쿼리를 위해 limit, offset 옵션을 추가하도록 쿼리문 수정
	var buf bytes.Buffer
	buf.WriteString(syntax)
	buf.WriteString(" LIMIT ? OFFSET ?")
	syntax = buf.String()
	// 수정된 쿼리문을 이용하여 쿼리 준비
	stmt, err := db.Prepare(syntax)
	if err != nil {
		return false, err
	}

	// 쿼리 수행 (병렬 처리)
	for i := uint64(0); i < nProc; i++ {
		go parallelProcess(stmt, dataQueue, nProcQuery, blockSize, (i * blockSize))
	}

	return true, nil
}

/* [Function] Create query syntax (dMES 전용) */
func CreateQuerySyntax(options map[string]interface{}, params []string) (string, error) {
	// 쿼리문 생성을 위한 데이터베이스 및 테이블, 속성 정보 추출
	conn := options["conn"].(map[string]interface{})
	attributes := options["attributes"].(map[string]interface{})
	conditions := options["conditions"].([]interface{})

	// 기본 데이터베이스 정보와 동의 내역 데이터베이스 정보
	baseTable := conn["database"].(string) + "." + conn["table"].(string)
	consentTable := ""
	// 반출할 속성 추출 및 조건 구문 생성
	attributesToExtract := make([]string, 0)
	joinSyntax := ""
	// 반출할 필드들과 쿼리 조건 생성
	for key, value := range attributes {
		detail := value.(map[string]interface{})
		// Verify export check
		if detail["isExport"].(bool) {
			attributesToExtract = append(attributesToExtract, baseTable+"."+key)
			// Check consent skip
			if detail["isPii"].(bool) && !detail["isConsentSkip"].(bool) {
				consentTable = detail["consentDatabase"].(string) + "." + detail["consentTable"].(string)
				if joinSyntax != "" {
					joinSyntax += " AND "
				}
				joinSyntax += consentTable + "." + key + "=1 AND ADD_MONTHS(TO_DATE(" + baseTable + ".LAST_ACCESSED), " + strconv.FormatFloat(detail["legalDuration"].(float64), 'f', 0, 64) + ") > NOW()"
			}
		}
	}
	// 사용자 지정 조건 생성
	var paramsIndex = 0
	conditionSyntax := ""
	for idx, elem := range conditions {
		condition := elem.(map[string]interface{})

		operator := condition["operator"].(string)
		if idx > 0 {
			conditionSyntax += " " + condition["connection"].(string)
		}
		// Fixed 여부
		value := condition["value"].(string)
		if !condition["fixed"].(bool) {
			if params == nil {
				value = "?"
			} else {
				value = params[paramsIndex]
				paramsIndex++
			}
		}
		// Create condition syntax
		if operator == "=" || operator == "!=" {
			if value == "?" {
				conditionSyntax += " " + baseTable + "." + condition["attribute"].(string) + " " + operator + " ?"
			} else if _, err := strconv.ParseFloat(condition["value"].(string), 64); err == nil {
				conditionSyntax += " " + baseTable + "." + condition["attribute"].(string) + " " + operator + " " + value
			} else {
				conditionSyntax += " " + baseTable + "." + condition["attribute"].(string) + " " + operator + " '" + value + "'"
			}
		} else if operator == "like" || operator == "not like" {
			if value == "?" {
				conditionSyntax += " " + baseTable + "." + condition["attribute"].(string) + " " + operator + " '%?%'"
			} else {
				conditionSyntax += " " + baseTable + "." + condition["attribute"].(string) + " " + operator + " '%" + value + "%'"
			}
		} else {
			if value == "?" {
				conditionSyntax += " " + baseTable + "." + condition["attribute"].(string) + " " + operator + " ?"
			} else {
				conditionSyntax += " " + baseTable + "." + condition["attribute"].(string) + " " + operator + " " + value
			}
		}
	}

	// 추출된 정보들을 이용하여 쿼리 생성
	var buffer bytes.Buffer
	buffer.WriteString("SELECT ")
	for i := range attributesToExtract {
		buffer.WriteString(attributesToExtract[i])
		if i < len(attributesToExtract)-1 {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(" FROM ")
	buffer.WriteString(baseTable)
	// Inner consent table
	if consentTable != "" {
		buffer.WriteString(" INNER JOIN ")
		buffer.WriteString(consentTable)
		buffer.WriteString(" ON ")
		buffer.WriteString(baseTable)
		buffer.WriteString(".PROFILES_ID=")
		buffer.WriteString(consentTable)
		buffer.WriteString(".PROFILES_ID")
	}
	// Condition
	if joinSyntax != "" || conditionSyntax != "" {
		buffer.WriteString(" WHERE ")
		if joinSyntax != "" {
			buffer.WriteString(joinSyntax)
			buffer.WriteString(" AND")
		}
		if conditionSyntax != "" {
			buffer.WriteString(conditionSyntax)
		}
	}
	return buffer.String(), nil
}

/* [Internal function] 병렬 쿼리 (변환 처리 포함) */
func parallelProcessC(stmt *sql.Stmt, dataQueue chan<- []string, nProcQuery chan<- bool, blockSize uint64, offset uint64) {
	// Query
	rows, err := stmt.Query(blockSize, offset)
	catchError(err)

	// Get column types
	cTypes, err := rows.ColumnTypes()
	catchError(err)
	// Create interface array to convert type
	columns := make([]interface{}, len(cTypes))
	for i := range columns {
		switch cTypes[i].ScanType().String() {
		case "int", "int8", "int16", "int32", "int64":
			columns[i] = new(int64)
		case "uint", "uint8", "uint16", "uint32", "uint64":
			columns[i] = new(uint64)
		case "float32", "float64":
			columns[i] = new(float64)
		case "bool":
			columns[i] = new(bool)
		case "driver.Decimal":
			columns[i] = new(driver.Decimal)
		case "string":
			columns[i] = new(string)
		case "time.Time":
			columns[i] = new(time.Time)
		default:
			log.Println("New type:", cTypes[i].ScanType().String())
			columns[i] = new(interface{})
		}
	}
	// Get row data
	for rows.Next() {
		// Scan
		rows.Scan(columns...)
		// Convert
		converted := make([]string, len(columns))
		for i := range columns {
			switch cTypes[i].ScanType().String() {
			case "int", "int8", "int16", "int32", "int64":
				converted[i] = strconv.FormatInt(*columns[i].(*int64), 10)
			case "uint", "uint8", "uint16", "uint32", "uint64":
				converted[i] = strconv.FormatUint(*columns[i].(*uint64), 10)
			case "float32", "float64":
				converted[i] = strconv.FormatFloat(*columns[i].(*float64), 'f', -6, 64)
			case "bool":
				converted[i] = strconv.FormatBool(*columns[i].(*bool))
			case "driver.Decimal":
				converted[i] = big.NewFloat(0).SetRat((*big.Rat)(columns[i].(*driver.Decimal))).String()
			case "string":
				converted[i] = *columns[i].(*string)
			case "time.Time":
				converted[i] = (columns[i].(*time.Time).Format("2006-01-02T15:04:05"))
			default:
				converted[i] = "*"
			}
		}
		// Send data in queue(channel)
		dataQueue <- converted
	}

	printLog("debug", "Routine exit")
	// Return query and convert result
	if err := rows.Err(); err != nil {
		printLog("error", err.Error())
		nProcQuery <- false
	} else {
		nProcQuery <- true
	}
}

/* [Internal function] 병렬 쿼리 (변환 처리 포함) */
func parallelProcess(stmt *sql.Stmt, dataQueue chan<- []string, nProcQuery chan<- bool, blockSize uint64, offset uint64) {
	// Query
	rows, err := stmt.Query(blockSize, offset)
	catchError(err)

	// Get column types
	cTypes, err := rows.Columns()
	catchError(err)
	columns := make([]interface{}, len(cTypes))

	// Get row data
	cnt := 0
	for rows.Next() {
		strResult := make([]string, len(cTypes))
		for i := range cTypes {
			columns[i] = &strResult[i]
		}
		// Scan
		rows.Scan(columns...)
		dataQueue <- strResult
		cnt++
	}

	printLog("debug", "Routine(Query) exit (DataCount:"+strconv.Itoa(cnt)+")")
	// Return query and convert result
	if err := rows.Err(); err != nil {
		printLog("error", err.Error())
		nProcQuery <- false
	} else {
		nProcQuery <- true
	}
}

/* [Internal function] Check file existance */
func checkFileExistance(requestID string, filename string) (string, error) {
	// 현재 작업 경로 추출
	workspace, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// 파일 경로 생성
	filePath := path.Join(workspace, "./resources/processed", requestID, filename)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return "", err
	} else {
		return filePath, nil
	}
}

/* [Internal function] Get file */
func GetOptions(requestID string) (map[string]interface{}, error) {
	// 요청된 requestID에 대한 반출 조회
	optionfilePath, err := checkFileExistance(requestID, "query.json")
	if err != nil {
		return nil, err
	}
	// 파일 열기
	file, err := os.Open(optionfilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// 데이터 추출을 위한 Reader와 추출된 데이터를 저장하기 위한 Buffer 생성
	reader := bufio.NewReader(file)
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, fileInfo.Size())
	// 데이터 읽기
	reader.Read(buffer)
	// []byte 데이터를 json 데이터로 변환
	var data map[string]interface{}
	json.Unmarshal(buffer, &data)

	return data, nil
}

/* [Function] Get queryed result total data size */
func GetDataSize(db *sql.DB, query string) (uint64, error) {
	log.Print(query)
	log.Print("")
	// Search index for modify query
	subsequentIndex := strings.Index(query, "FROM") + 4
	// Combine strings (add count syntax)
	var buf bytes.Buffer
	buf.WriteString("SELECT COUNT(*) FROM")
	buf.WriteString(query[subsequentIndex:])
	modifiedQuery := buf.String()
	// Execute query using modified query syntax
	row := db.QueryRow(modifiedQuery)
	// Get query result
	var rawResult string
	row.Scan(&rawResult)
	result, err := strconv.Atoi(rawResult)
	if err != nil {
		return uint64(0), err
	}
	// Return
	return uint64(result), nil
}

func GetDataColumns(db *sql.DB, query string) ([]string, error) {
	var buf bytes.Buffer
	buf.WriteString(query)
	buf.WriteString(" LIMIT 1")
	modifiedQuery := buf.String()

	rows, err := db.Query(modifiedQuery)
	if err != nil {
		return nil, err
	}

	headerInfo, err := rows.Columns()
	if err != nil {
		return nil, err
	} else {
		return headerInfo, nil
	}
}

/* [Internal function] Print log */
func printLog(logType string, message string) {
	var buf bytes.Buffer
	// matching by type
	switch logType {
	case "notice": // Color is GREEN
		buf.WriteString("\x1b[32m[NOTICE] ")
		buf.WriteString(message)
		buf.WriteString("\x1b[0m")
	case "debug": // Color is MAGENTA
		buf.WriteString("\x1b[35m[DEBUG] ")
		buf.WriteString(message)
		buf.WriteString("\x1b[0m")
	case "error": // Color is RED
		buf.WriteString("\x1b[31m[ERROR] ")
		buf.WriteString(message)
		buf.WriteString("\x1b[0m")
	default: // Color is DEFAULT (WHITE)
		buf.WriteString("\x1b[0m[ERROR] ")
		buf.WriteString(message)
		buf.WriteString("\x1b[0m")
	}
	// Print
	log.Print(buf.String())
}

/* [Internal function] Catch error */
func catchError(err interface{}) {
	if err != nil {
		panic(err)
	}
}

// /* [Function] Parallel processing */
// func ParallelQuery(db *sql.DB, query string, dataQueue chan []string, procQueue chan bool, nQueryProc uint64, blockSize uint64) {
// 	// Get queryed result total size
// 	// totalDataSize, err := GetDataSize(db, query)
// 	// if totalDataSize == 0 {
// 	// 	printLog("error", "Query result data size is 0...")
// 	// 	return
// 	// }
// 	// printLog("debug", "Total data size: " + strconv.FormatUint(totalDataSize, 10))
// 	// // Calculate loop
// 	// nQueryProc := totalDataSize / blockSize
// 	// if remainSize := totalDataSize % blockSize; remainSize > 0 {
// 	// 	nQueryProc += 1
// 	// }

// 	// Set process count
// 	runtime.GOMAXPROCS(runtime.NumCPU() * 4)

// 	// Modify query syntax (add options)
// 	var buf bytes.Buffer
// 	buf.WriteString(query)
// 	buf.WriteString(" LIMIT ? OFFSET ?")
// 	query = buf.String()
// 	// Prepare
// 	stmt, err := db.Prepare(query)
// 	catchError(err)

// 	// // Converted uint32 to uint64
// 	// blockSize := uint64(BLOCK_SIZE)
// 	// // Calculate loop
// 	// nQueryProc := totalDataSize / blockSize
// 	// if remainSize := totalDataSize % blockSize; remainSize > 0 {
// 	// 	nQueryProc += 1
// 	// }
// 	// Create go-routine
// 	for i := uint64(0); i < nQueryProc; i++ {
// 		go executeQuery(stmt, dataQueue, procQueue, blockSize, (i * blockSize))
// 	}

// 	// cp := uint64(0)
// 	// ProcLoop:
// 	// for {
// 	// 	select {
// 	// 		case _, ok := <-dataQueue:
// 	// 			if !ok {
// 	// 				printLog("debug", "END")
// 	// 				defer stmt.Close()
// 	// 				defer db.Close()

// 	// 				break ProcLoop
// 	// 			}
// 	// 		case <-procQueue:
// 	// 			cp++
// 	// 			if nQueryProc <= cp {
// 	// 				close(dataQueue)
// 	// 			}
// 	// 	}
// 	// }
// }

// /* [Internal function] Execute query */
// func executeQuery(stmt *sql.Stmt, dataQueue chan<- []string, procQueue chan<- bool, limit uint64, offset uint64) {
// 	// Query
// 	rows, err := stmt.Query(limit, offset)
// 	catchError(err)

// 	// Get column types
// 	cTypes, err := rows.ColumnTypes()
// 	catchError(err)
// 	// Create interface array to convert type
// 	columns := make([]interface{}, len(cTypes))
// 	for i := range(columns) {
// 		switch cTypes[i].ScanType().String() {
// 			case "int", "int8", "int16", "int32", "int64":
// 				columns[i] = new(int64)
// 			case "uint", "uint8", "uint16", "uint32", "uint64":
// 				columns[i] = new(uint64)
// 			case "float32", "float64":
// 				columns[i] = new(float64)
// 			case "bool":
// 				columns[i] = new(bool)
// 			case "driver.Decimal":
// 				columns[i] = new(driver.Decimal)
// 			case "string":
// 				columns[i] = new(string)
// 			case "time.Time":
// 				columns[i] = new(time.Time)
// 			default:
// 				log.Println("New type:", cTypes[i].ScanType().String())
// 				columns[i] = new(interface{})
// 		}
// 	}
// 	// Get row data
// 	for rows.Next() {
// 		// Scan
// 		rows.Scan(columns...)
// 		// Convert
// 		converted := make([]string, len(columns))
// 		for i := range(columns) {
// 			switch cTypes[i].ScanType().String() {
// 				case "int", "int8", "int16", "int32", "int64":
// 					converted[i] = strconv.FormatInt(*columns[i].(*int64), 10)
// 				case "uint", "uint8", "uint16", "uint32", "uint64":
// 					converted[i] = strconv.FormatUint(*columns[i].(*uint64), 10)
// 				case "float32", "float64":
// 					converted[i] = strconv.FormatFloat(*columns[i].(*float64), 'f', -6, 64)
// 				case "bool":
// 					converted[i] = strconv.FormatBool(*columns[i].(*bool))
// 				case "driver.Decimal":
// 					converted[i] = big.NewFloat(0).SetRat((*big.Rat)(columns[i].(*driver.Decimal))).String()
// 				case "string":
// 					converted[i] = *columns[i].(*string)
// 				case "time.Time":
// 					converted[i] = (columns[i].(*time.Time).Format("2006-01-02T15:04:05"))
// 				default:
// 					converted[i] = "*"
// 			}
// 		}
// 		// Send data in queue(channel)
// 		dataQueue <- converted
// 	}

// 	printLog("debug", "Routine exit")
// 	// Return query and convert result
// 	if err := rows.Err(); err != nil {
// 		printLog("error", err.Error())
// 		procQueue <- false
// 	} else {
// 		procQueue <- true
// 	}
// }
