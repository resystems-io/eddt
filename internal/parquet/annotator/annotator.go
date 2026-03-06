package annotator

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"reflect"
	"strings"
)

// Task represents a source and destination for Parquet annotation.
type Task struct {
	In   io.Reader  `parquet:"name=In"`
	Out  io.Writer  `parquet:"name=Out"`
	Done chan error `parquet:"name=Done"` // Signaled when task processing completes
}

// ParquetAnnotator is a utility that can be parameterized by setting config parameters.
type ParquetAnnotator struct {
	Verbose       bool     `parquet:"name=Verbose, type=BOOLEAN"` // Example config
	TargetStructs []string `parquet:"name=TargetStructs, type=LIST"`
}

// NewParquetAnnotator creates a new ParquetAnnotator.
func NewParquetAnnotator() *ParquetAnnotator {
	return &ParquetAnnotator{}
}

// Start launches the annotator processing loop.
// It takes an end channel to shut it down, and returns an input channel for Tasks.
func (a *ParquetAnnotator) Start(end <-chan struct{}) chan<- Task {
	in := make(chan Task)
	go func() {
		for {
			select {
			case <-end:
				return
			case task, ok := <-in:
				if !ok {
					return
				}
				if task.In != nil && task.Out != nil {
					err := a.Annotate(task.In, task.Out)
					if err != nil && a.Verbose {
						fmt.Printf("Error annotating: %v\n", err)
					}
					if task.Done != nil {
						task.Done <- err
						close(task.Done)
					}
				} else if task.Done != nil {
					task.Done <- fmt.Errorf("invalid task: In or Out is nil")
					close(task.Done)
				}
			}
		}
	}()
	return in
}

// isTargetStruct checks if the given type name is in the TargetStructs list.
func (a *ParquetAnnotator) isTargetStruct(name string) bool {
	if len(a.TargetStructs) == 0 {
		return true // No filter specified, annotate all
	}
	for _, ts := range a.TargetStructs {
		if ts == name {
			return true
		}
	}
	return false
}

// Annotate reads Go source from In, adds Parquet tags to structs, and writes to Out.
func (a *ParquetAnnotator) Annotate(in io.Reader, out io.Writer) error {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(in)
	if err != nil {
		return err
	}

	fset := token.NewFileSet()
	// Parse the file. We want to preserve comments.
	file, err := parser.ParseFile(fset, "", buf.Bytes(), parser.ParseComments)
	if err != nil {
		return err
	}

	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.TypeSpec:
			// Check if this type specification is a struct
			if structType, ok := node.Type.(*ast.StructType); ok {
				// If TargetStructs is provided, only process matching structs
				if a.isTargetStruct(node.Name.Name) {
					for _, field := range structType.Fields.List {
						a.addParquetTag(field)
					}
				}
			}
		}
		return true
	})

	return format.Node(out, fset, file)
}

func (a *ParquetAnnotator) addParquetTag(field *ast.Field) {
	if len(field.Names) == 0 {
		return // Anonymous fields skipped for simplicity in this baseline
	}

	fieldName := field.Names[0].Name
	if !ast.IsExported(fieldName) {
		return
	}

	tagValue := ""
	if field.Tag != nil {
		tagValue = strings.Trim(field.Tag.Value, "`")
	}

	structTag := reflect.StructTag(tagValue)
	parquetTag := structTag.Get("parquet")

	if parquetTag != "" {
		return // Parquet tag already exists, do not modify
	}

	// Use field name as default, check JSON for overrides
	nameKey := fieldName
	jsonTag := structTag.Get("json")
	if jsonTag != "" {
		parts := strings.Split(jsonTag, ",")
		if parts[0] != "" && parts[0] != "-" {
			nameKey = parts[0]
		}
	}

	parquetOpts := []string{fmt.Sprintf("name=%s", nameKey)}

	// Infer simple types
	var typeHint, logicalHint string
	var checkType func(expr ast.Expr)
	checkType = func(expr ast.Expr) {
		switch t := expr.(type) {
		case *ast.Ident:
			switch t.Name {
			case "string":
				typeHint = "BYTE_ARRAY"
				logicalHint = "String"
			case "int64", "int":
				typeHint = "INT64"
			case "int32":
				typeHint = "INT32"
			case "bool":
				typeHint = "BOOLEAN"
			case "float32":
				typeHint = "FLOAT"
			case "float64":
				typeHint = "DOUBLE"
			}
		case *ast.StarExpr:
			checkType(t.X)
		case *ast.ArrayType:
			if ident, ok := t.Elt.(*ast.Ident); ok && ident.Name == "byte" {
				typeHint = "BYTE_ARRAY"
			} else {
				typeHint = "LIST"
			}
		case *ast.SelectorExpr:
			if ident, ok := t.X.(*ast.Ident); ok {
				if ident.Name == "time" && t.Sel.Name == "Time" {
					typeHint = "INT64"
					logicalHint = "TIMESTAMP_MILLIS"
				}
			}
		}
	}
	checkType(field.Type)

	if typeHint != "" {
		parquetOpts = append(parquetOpts, fmt.Sprintf("type=%s", typeHint))
	}
	if logicalHint != "" {
		parquetOpts = append(parquetOpts, fmt.Sprintf("logicaltype=%s", logicalHint))
	}

	newParquetTag := strings.Join(parquetOpts, ", ")

	if field.Tag == nil {
		field.Tag = &ast.BasicLit{Kind: token.STRING}
	}

	if tagValue == "" {
		field.Tag.Value = fmt.Sprintf("`parquet:\"%s\"`", newParquetTag)
	} else {
		field.Tag.Value = fmt.Sprintf("`%s parquet:\"%s\"`", tagValue, newParquetTag)
	}
}
