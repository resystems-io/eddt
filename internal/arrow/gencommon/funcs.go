package gencommon

import "text/template"

// TemplateFuncs returns a base template.FuncMap shared by all Arrow code
// generators. It provides:
//
//   - "dict": constructs a map[string]any from a flat key/value sequence,
//     used to pass multiple named arguments to named sub-templates (since
//     Go's text/template only supports a single pipeline value per call).
//   - "add": integer addition, used for depth-tracking in recursive sub-
//     templates.
//
// Generator-specific functions (e.g. stripPtr, isDictCandidate, repeat for
// readergen) should be composed into the map returned by this function rather
// than duplicated independently:
//
//	m := gencommon.TemplateFuncs()
//	m["myFunc"] = myFunc
//	tmpl := template.Must(template.New("...").Funcs(m).Parse(...))
func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"dict": func(values ...any) map[string]any {
			m := make(map[string]any)
			for i := 0; i < len(values); i += 2 {
				m[values[i].(string)] = values[i+1]
			}
			return m
		},
		"add": func(a, b int) int { return a + b },
	}
}
