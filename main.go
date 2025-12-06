package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

type Input struct {
	Params []struct {
		InputName string `json:"inputname"`
		CompValue string `json:"compvalue"`
	} `json:"params"`
}

type Output struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error"`
}

func main() {
	var input Input
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("failed to decode input: %v", err)})
		return
	}

	var (
		host       string
		port       int
		username   string
		password   string
		dbname     string
		sslmode    = "disable"
		dataType   = "query" // query, table, stored_procedure, stored_function
		objectName string
		query      string
		parameters string // JSON array of arguments
	)

	// Extract parameters
	for _, p := range input.Params {
		val := strings.TrimSpace(p.CompValue)
		switch strings.ToLower(p.InputName) {
		case "host":
			host = val
		case "port":
			fmt.Sscanf(val, "%d", &port)
		case "username":
			username = val
		case "password":
			password = val
		case "dbname":
			dbname = val
		case "sslmode":
			if val != "" {
				sslmode = val
			}
		case "data_type":
			if val != "" {
				dataType = strings.ToLower(val)
			}
		case "object_name":
			objectName = val
		case "query":
			query = val
		case "parameters":
			parameters = val
		}
	}

	// Validate connection params
	if host == "" || username == "" || dbname == "" {
		json.NewEncoder(os.Stdout).Encode(Output{Error: "host, username, and dbname are required"})
		return
	}
	if port == 0 {
		port = 5432
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", host, port, username, password, dbname, sslmode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("failed to connect: %v", err)})
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("failed to ping db: %v", err)})
		return
	}

	var rows *sql.Rows
	var execResult sql.Result
	isSelect := false

	switch dataType {
	case "table":
		if objectName == "" {
			json.NewEncoder(os.Stdout).Encode(Output{Error: "object_name is required for table"})
			return
		}
		rows, err = db.Query(fmt.Sprintf("SELECT * FROM %s", objectName))
		isSelect = true

	case "stored_procedure":
		if objectName == "" {
			json.NewEncoder(os.Stdout).Encode(Output{Error: "object_name is required for stored_procedure"})
			return
		}
		args, err := parseArgs(parameters)
		if err != nil {
			json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("invalid parameters: %v", err)})
			return
		}

		placeholders := make([]string, len(args))
		for i := range args {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		q := fmt.Sprintf("CALL %s(%s)", objectName, strings.Join(placeholders, ","))
		rows, err = db.Query(q, args...)
		isSelect = true

	case "stored_function":
		if objectName == "" {
			json.NewEncoder(os.Stdout).Encode(Output{Error: "object_name is required for stored_function"})
			return
		}
		args, err := parseArgs(parameters)
		if err != nil {
			json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("invalid parameters: %v", err)})
			return
		}

		placeholders := make([]string, len(args))
		for i := range args {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		// SELECT * FROM func(args) is safer for returning tables
		q := fmt.Sprintf("SELECT * FROM %s(%s)", objectName, strings.Join(placeholders, ","))
		rows, err = db.Query(q, args...)
		isSelect = true

	case "query":
		fallthrough
	default:
		if query == "" {
			json.NewEncoder(os.Stdout).Encode(Output{Error: "query is required"})
			return
		}
		isSelect = strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") || strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "WITH")

		if !isSelect {
			// Check other read commands
			cmd := strings.ToUpper(strings.TrimSpace(query))
			if strings.HasPrefix(cmd, "SHOW") || strings.HasPrefix(cmd, "EXPLAIN") || strings.HasPrefix(cmd, "CALL") {
				isSelect = true
			}
		}

		if isSelect {
			rows, err = db.Query(query)
		} else {
			execResult, err = db.Exec(query)
		}
	}

	if err != nil {
		json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("execution error: %v", err)})
		return
	}

	if isSelect && rows != nil {
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("columns error: %v", err)})
			return
		}

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			columnPointers := make([]interface{}, len(columns))
			for i := range columns {
				columnPointers[i] = new(interface{})
			}

			if err := rows.Scan(columnPointers...); err != nil {
				json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("scan error: %v", err)})
				return
			}

			m := make(map[string]interface{})
			for i, colName := range columns {
				val := *(columnPointers[i].(*interface{}))

				// Handle []byte for strings and other types depending on driver
				if b, ok := val.([]byte); ok {
					m[colName] = string(b)
				} else {
					m[colName] = val
				}
			}
			results = append(results, m)
		}
		json.NewEncoder(os.Stdout).Encode(Output{Result: results})

	} else if execResult != nil {
		affected, _ := execResult.RowsAffected()
		// LastInsertId is not supported by lib/pq usually, returns 0 error
		json.NewEncoder(os.Stdout).Encode(Output{Result: map[string]int64{
			"rows_affected": affected,
		}})
	} else {
		json.NewEncoder(os.Stdout).Encode(Output{Result: "OK"})
	}
}

func parseArgs(paramStr string) ([]interface{}, error) {
	if paramStr == "" {
		return []interface{}{}, nil
	}
	var args []interface{}
	if err := json.Unmarshal([]byte(paramStr), &args); err != nil {
		return nil, err
	}
	return args, nil
}
