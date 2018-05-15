package main

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	solr "github.com/emmansun/Go-Solr"
	cli "gopkg.in/urfave/cli.v1"
)

const (
	CSVFormat  = "csv"
	JSONFormat = "json"

	MaxTryRecords = 100
	MaxGoRoutines = 4
)

var (
	hostFlag = cli.StringFlag{
		Name:  "host",
		Usage: "host name. Example: \"localhost\"",
	}
	portFlag = cli.IntFlag{
		Name:  "port",
		Usage: "port, default is 80.",
		Value: 80,
	}
	contextFlag = cli.StringFlag{
		Name:  "context",
		Usage: "context name, default is solr. part of final url",
	}
	outputFlag = cli.StringFlag{
		Name:  "output",
		Usage: "Output file",
	}
	formatFlag = cli.StringFlag{
		Name:  "format",
		Usage: "Output file format. Can be \"json\" or \"csv\". Default is \"json\"",
		Value: JSONFormat,
	}
)

type commandInfo struct {
	host    string
	port    int
	output  string
	format  string
	context string
}

type docField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type docSchema []docField

// Len is the number of elements in the collection.
func (schema docSchema) Len() int {
	return len(schema)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (schema docSchema) Less(i, j int) bool {
	return strings.Compare(schema[i].Name, schema[j].Name) < 0
}

// Swap swaps the elements with indexes i and j.
func (schema docSchema) Swap(i, j int) {
	temp := schema[i]
	schema[i] = schema[j]
	schema[j] = temp
}

var tasks chan string

func addIfNotExists(schema *docSchema, field *docField, fieldSet map[string]struct{}) {
	if _, ok := fieldSet[field.Name]; !ok {
		fieldSet[field.Name] = struct{}{}
		*schema = append(*schema, *field)
	}
}

func isInt64(val float64) bool {
	return val == float64(int(val))
}

func isInt32(val float32) bool {
	return val == float32(int(val))
}

func parseNumberType(lowcaseName string, value float64) string {
	switch {
	case strings.HasSuffix(lowcaseName, "_i") || strings.HasSuffix(lowcaseName, "_is") || strings.HasSuffix(lowcaseName, "_ilist[]"):
		return "INTEGER"
	case isInt64(value):
		return "LONG"
	default:
		return "DOUBLE"
	}
}

func parseStringType(lowcaseName, value string) string {
	switch {
	case strings.HasSuffix(lowcaseName, "_dt") || strings.HasSuffix(lowcaseName, "_dts") || strings.HasSuffix(lowcaseName, "_dtlist[]"):
		return "DATE"
	case strings.HasSuffix(lowcaseName, "_bin"):
		return "BINARY"
	default:
		return "STRING"
	}
}

func checkFieldType(solrSchema *solr.Schema, field *docField) bool {
	if solrSchema == nil || (len(solrSchema.DynamicField) == 0 && len(solrSchema.Field) == 0) {
		return false
	}
	fieldName := field.Name
	if strings.HasSuffix(fieldName, "[]") {
		fieldName = fieldName[0 : len(fieldName)-2]
	}
	if len(solrSchema.Field) > 0 {
		for _, f := range solrSchema.Field {
			if fieldName == f.Name || strings.HasSuffix(fieldName, "."+f.Name) {
				field.Type = strings.ToUpper(f.Type)
				return true
			}
		}
	}
	if len(solrSchema.DynamicField) > 0 {
		for _, f := range solrSchema.DynamicField {
			if strings.HasPrefix(f.Name, "*") && strings.HasSuffix(fieldName, f.Name[1:]) {
				field.Type = strings.ToUpper(f.Type)
				return true
			}
			if strings.HasSuffix(f.Name, "*") && strings.HasPrefix(fieldName, f.Name[:len(f.Name)-1]) {
				field.Type = strings.ToUpper(f.Type)
				return true
			}
		}
	}
	return false
}

func getSchema(solrSchema *solr.Schema, prefix string, object interface{}, schema *docSchema, fieldSet map[string]struct{}) {
	if object == nil {
		return
	}
	field := new(docField)
	if prefix != "" {
		field.Name = prefix
	}
	lowcaseName := strings.ToLower(field.Name)
	switch object.(type) {
	case float64:
		if !checkFieldType(solrSchema, field) {
			field.Type = parseNumberType(lowcaseName, object.(float64))
		}
		addIfNotExists(schema, field, fieldSet)
		break
	case string:
		if !checkFieldType(solrSchema, field) {
			field.Type = parseStringType(lowcaseName, object.(string))
		}
		addIfNotExists(schema, field, fieldSet)
		break
	case bool:
		field.Type = "BOOL"
		addIfNotExists(schema, field, fieldSet)
		break
	case []interface{}:
		field.Type = "ARRAY"
		addIfNotExists(schema, field, fieldSet)
		for i, v := range object.([]interface{}) {
			if i < MaxTryRecords {
				getSchema(solrSchema, field.Name+"[]", v, schema, fieldSet)
			} else {
				break
			}
		}
		break
	case map[string]interface{}:
		getStructureSchema(solrSchema, field.Name, solr.Document{Fields: object.(map[string]interface{})}, schema, fieldSet)
		break
	default:
		if !checkFieldType(solrSchema, field) {
			field.Type = "UNKNOWN"
		}
		addIfNotExists(schema, field, fieldSet)
		log.Printf("%v, Unknown=%v\n", field.Name, reflect.TypeOf(object))
		break
	}
}

func getStructureSchema(solrSchema *solr.Schema, prefix string, doc solr.Document, schema *docSchema, fieldSet map[string]struct{}) {
	for n, v := range doc.Doc() {
		if v == nil {
			continue
		}
		name := prefix
		if prefix == "" {
			name = n
		} else {
			name = prefix + "." + n
		}
		getSchema(solrSchema, name, v, schema, fieldSet)
	}
}

func buildQuery() *solr.Query {
	query := solr.Query{Params: solr.URLParamMap{
		"q": []string{"*:*"},
	},
		Rows: MaxTryRecords,
	}
	return &query
}

func getSchemaDefinitionFromFile(conn *solr.Connection) (*solr.Schema, error) {
	schemaFile, err := conn.AdminGetCoreFile(solr.SchemaFile)
	if err != nil {
		return nil, err
	}
	schema := new(solr.Schema)
	err = xml.Unmarshal(schemaFile, schema)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func getSchemaDefinitionFromAPI(conn *solr.Connection) (*solr.Schema, error) {
	schema, err := conn.AdminListSchemaFields(true)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func getSchemaDefinition(conn *solr.Connection) (*solr.Schema, error) {
	configFile, err := conn.AdminGetCoreFile(solr.ConfigFile)
	if err != nil {
		return nil, err
	}
	config := new(solr.SolrConfig)
	err = xml.Unmarshal(configFile, config)
	if err != nil {
		return nil, err
	}
	if config.SchemaFactory.Class == solr.ClassicIndexSchemaFactory {
		return getSchemaDefinitionFromFile(conn)
	}
	return getSchemaDefinitionFromAPI(conn)
}

func genCoreSchema(dbSchema map[string]docSchema, comm *commandInfo, coreName string) {
	fieldSet := make(map[string]struct{})
	conn, err := solr.InitWithCtx(comm.host, comm.port, comm.context, coreName)
	if err != nil {
		log.Fatalln(err)
	}
	solrSchema, err := getSchemaDefinition(conn)
	if err != nil {
		log.Printf("Failed to get schema fields %v\n", err)
	}
	resp, err := conn.Select(buildQuery())
	if err != nil {
		log.Fatalln(err)
	}
	var colSchema = docSchema{}
	if resp.Results.NumFound > 0 {
		for _, result := range resp.Results.Collection {
			getStructureSchema(solrSchema, "", result, &colSchema, fieldSet)
		}
		if len(colSchema) > 1 {
			sort.Sort(colSchema)
		}
	}
	dbSchema[coreName] = colSchema
}

func getContextSchema(comm *commandInfo) map[string]docSchema {
	log.Printf("Extract schema for context %v\n", comm.context)
	defer func(start time.Time) {
		log.Printf("Extract schema for database %v done, used time %v\n", comm.context, time.Now().Sub(start))
	}(time.Now())
	dbSchemas := make(map[string]docSchema)

	collectionNames, err := solr.AdminListCores(comm.host, comm.port, comm.context)
	if err != nil {
		log.Fatal(err)
	}
	if len(collectionNames) > 0 {
		var done sync.WaitGroup
		tasks = make(chan string, len(collectionNames))
		for _, collectionName := range collectionNames {
			tasks <- collectionName
		}
		close(tasks)
		routines := MaxGoRoutines
		if routines > len(collectionNames) {
			routines = len(collectionNames)
		}
		for i := 1; i <= MaxGoRoutines; i++ {
			done.Add(1)
			go func(i int) {
				defer done.Done()
				for {
					collectionName, ok := <-tasks
					if !ok {
						return
					}
					startTime := time.Now()
					genCoreSchema(dbSchemas, comm, collectionName)
					log.Printf("Go Routine %v, Extract schema for collection %v, used time %v.\n", i, collectionName, time.Now().Sub(startTime))
				}
			}(i)
		}
		done.Wait()
	}
	return dbSchemas
}

func exportJSON(cmdInfo *commandInfo, schema map[string]docSchema) error {
	schemaJSON, err := json.Marshal(schema)
	if err == nil {
		return ioutil.WriteFile(cmdInfo.output, schemaJSON, 0644)
	}
	return err
}

func exportCSV(cmdInfo *commandInfo, schema map[string]docSchema) error {
	f, err := os.Create(cmdInfo.output)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	for c, fields := range schema {
		if len(fields) > 0 {
			for _, f := range fields {
				err := writer.Write([]string{c, f.Name, f.Type})
				if err != nil {
					return err
				}
			}
		}
	}
	writer.Flush()
	return nil
}

func extractSchema(ctx *cli.Context) error {
	if ctx.NumFlags() == 0 {
		cli.ShowAppHelpAndExit(ctx, -1)
		return nil
	}
	cmdInfo := new(commandInfo)
	if !ctx.GlobalIsSet(hostFlag.Name) {
		log.Fatalf("%s is mandatory!", hostFlag.Name)
	}
	cmdInfo.host = ctx.GlobalString(hostFlag.Name)
	cmdInfo.format = formatFlag.Value
	if ctx.GlobalIsSet(formatFlag.Name) {
		cmdInfo.format = ctx.GlobalString(formatFlag.Name)
	}
	if cmdInfo.format != JSONFormat && cmdInfo.format != CSVFormat {
		cmdInfo.format = JSONFormat
	}
	if ctx.GlobalIsSet(contextFlag.Name) {
		cmdInfo.context = ctx.GlobalString(contextFlag.Name)
	}
	if cmdInfo.context == "" {
		cmdInfo.context = "solr"
	}
	if ctx.GlobalIsSet(portFlag.Name) {
		cmdInfo.port = ctx.GlobalInt(portFlag.Name)
	}
	if cmdInfo.port <= 0 {
		cmdInfo.port = 80
	}
	if !ctx.GlobalIsSet(outputFlag.Name) {
		log.Fatalf("%s is mandatory!", outputFlag.Name)
	}
	cmdInfo.output = ctx.GlobalString(outputFlag.Name)
	schema := getContextSchema(cmdInfo)
	if cmdInfo.format == JSONFormat {
		return exportJSON(cmdInfo, schema)
	}
	return exportCSV(cmdInfo, schema)
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	app := cli.NewApp()
	app.Name = "extract solr schema"
	app.Description = "extract solr schema"
	app.Flags = []cli.Flag{hostFlag, portFlag, contextFlag, outputFlag, formatFlag}
	app.Action = extractSchema
	err := app.Run(os.Args)
	if err != nil {
		log.Panic(err)
	}
}
