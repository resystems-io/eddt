package route

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"testing"

	"github.com/sergi/go-diff/diffmatchpatch"

	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/common/assert"
)

func ExampleCompiledRoute_regex() {

	input := "some.domain.prefix.${thing}.and.${13}.with.more.${other}.numbered.${17}.stuff"
	regex := "(?P<literal>[a-zA-Z0-9_.-]+)|(?P<ref>\\$\\{(?P<id>(?P<name>[a-z]+)|(?P<idx>[0-9]+))\\})"

	re := regexp.MustCompile(regex)
	matches := re.FindAllStringSubmatch(input, -1)

	// extract the named groups
	subnames := re.SubexpNames()
	for si, sn := range subnames {
		fmt.Printf("<%d> → <%s>\n", si, sn)
	}

	// show the input we are parsing
	fmt.Printf("%s\n\n", input)

	// unpack the input
	for i, match := range matches {
		fmt.Printf("[%d]", i)
		for j, m := range match {
			fmt.Printf(" [%2d <%s>=%22q]", j, subnames[j], m)
		}
		fmt.Printf("\n")
	}

	// Output:
	// <0> → <>
	// <1> → <literal>
	// <2> → <ref>
	// <3> → <id>
	// <4> → <name>
	// <5> → <idx>
	// some.domain.prefix.${thing}.and.${13}.with.more.${other}.numbered.${17}.stuff
	//
	// [0] [ 0 <>= "some.domain.prefix."] [ 1 <literal>= "some.domain.prefix."] [ 2 <ref>=                    ""] [ 3 <id>=                    ""] [ 4 <name>=                    ""] [ 5 <idx>=                    ""]
	// [1] [ 0 <>=            "${thing}"] [ 1 <literal>=                    ""] [ 2 <ref>=            "${thing}"] [ 3 <id>=               "thing"] [ 4 <name>=               "thing"] [ 5 <idx>=                    ""]
	// [2] [ 0 <>=               ".and."] [ 1 <literal>=               ".and."] [ 2 <ref>=                    ""] [ 3 <id>=                    ""] [ 4 <name>=                    ""] [ 5 <idx>=                    ""]
	// [3] [ 0 <>=               "${13}"] [ 1 <literal>=                    ""] [ 2 <ref>=               "${13}"] [ 3 <id>=                  "13"] [ 4 <name>=                    ""] [ 5 <idx>=                  "13"]
	// [4] [ 0 <>=         ".with.more."] [ 1 <literal>=         ".with.more."] [ 2 <ref>=                    ""] [ 3 <id>=                    ""] [ 4 <name>=                    ""] [ 5 <idx>=                    ""]
	// [5] [ 0 <>=            "${other}"] [ 1 <literal>=                    ""] [ 2 <ref>=            "${other}"] [ 3 <id>=               "other"] [ 4 <name>=               "other"] [ 5 <idx>=                    ""]
	// [6] [ 0 <>=          ".numbered."] [ 1 <literal>=          ".numbered."] [ 2 <ref>=                    ""] [ 3 <id>=                    ""] [ 4 <name>=                    ""] [ 5 <idx>=                    ""]
	// [7] [ 0 <>=               "${17}"] [ 1 <literal>=                    ""] [ 2 <ref>=               "${17}"] [ 3 <id>=                  "17"] [ 4 <name>=                    ""] [ 5 <idx>=                  "17"]
	// [8] [ 0 <>=              ".stuff"] [ 1 <literal>=              ".stuff"] [ 2 <ref>=                    ""] [ 3 <id>=                    ""] [ 4 <name>=                    ""] [ 5 <idx>=                    ""]
}

func TestCompliedRoute(t *testing.T) {
	t.Run("check-transform-subexp", func(t *testing.T) {
		// simply check that our hard-coded look-up groups match the regular expression we are using.
		re := regexp.MustCompile(TOKENISE_TRANSFORM_SUBJECT_REGEX)
		assert.EqualI(t, re.SubexpIndex(TRANSFORM_GROUP_IDX_ID), TRANSFORM_GROUP_IDX)
		assert.EqualI(t, re.SubexpIndex(TRANSFORM_GROUP_NAME_ID), TRANSFORM_GROUP_NAME)
		assert.EqualI(t, re.SubexpIndex(TRANSFORM_GROUP_LITERAL_ID), TRANSFORM_GROUP_LITERAL)
	})
	t.Run("check-relset-subexp", func(t *testing.T) {
		// simply check that our hard-coded look-up groups match the regular expression we are using.
		re := regexp.MustCompile(TOKENISE_RELSET_SUBJECT_REGEX)
		assert.EqualI(t, re.SubexpIndex(RELSET_GROUP_IDX_ID), RELSET_GROUP_IDX)
		assert.EqualI(t, re.SubexpIndex(RELSET_GROUP_LITERAL_ID), RELSET_GROUP_LITERAL)
	})
	t.Run("compile-route", func(t *testing.T) {

		expected := CompiledRoute{
			ID: "my-route",
			Instructions: []RouteInstruction{
				{Tokenise, 0, 0},
				{AppendEgressLiteral, 0, 0},
				{AppendRelLiteral, 1, 0},
				{AppendRelToken, 2, 0},
				{AppendRelLiteral, 2, 0},
				{FetchRelSet, 0, 0},
				{AppendEgressRelations, 0, 0},
				{AppendEgressLiteral, 3, 0},
				{AppendEgressToken, 3, 0},
				{AppendEgressLiteral, 4, 0},
				{AppendRelLiteral, 1, 1},
				{AppendRelToken, 5, 1},
				{AppendRelLiteral, 5, 1},
				{FetchRelSet, 1, 1},
				{AppendEgressRelations, 1, 1},
				{AppendEgressLiteral, 6, 0},
				{AppendEgressToken, 4, 0},
				{AppendEgressLiteral, 7, 0},
			},
			Literals: []string{
				"some.domain.prefix.",
				"some.domain.rel.",
				".set",
				".and.",
				".with.more.",
				".list",
				".numbered.",
				".stuff",
			},
		}

		route := contract.Route{
			ID:        "my-route",
			Disabled:  false,
			Match:     "some.domain.prefix.>",
			Transform: "some.domain.prefix.${thing}.and.${3}.with.more.${other}.numbered.${4}.stuff",
			References: map[string]string{
				"thing": "some.domain.rel.${2}.set",
				"other": "some.domain.rel.${5}.list",
			},
		}
		cr, err := Compile(&route)
		assert.NotNil(t, cr)
		assert.NoError(t, err)
		assert.EqualS(t, string(expected.ID), string(cr.ID))

		areEqual := reflect.DeepEqual(&expected, cr)
		assert.True(t, areEqual)
		if !areEqual {
			// highlight the differences
			expectedJSON, err := json.MarshalIndent(&expected, "", "  ")
			if err != nil {
				t.Errorf("JSON marshaling failed: %v", err)
			}
			crJSON, err := json.MarshalIndent(cr, "", "  ")
			if err != nil {
				t.Errorf("JSON marshaling failed: %v", err)
			}
			dmp := diffmatchpatch.New()
			diffs := dmp.DiffMain(string(crJSON), string(expectedJSON), false)
			t.Logf("\ndiff (see read/green formatting):\n%v\n", dmp.DiffPrettyText(diffs))
			patch := dmp.PatchMake(string(crJSON), diffs)
			t.Logf("\npatch:\n%v\n", dmp.PatchToText(patch))
		}

		t.Logf("%v", cr)
	})
}

func ExampleCompiledRoute_regex_namedpairs() {

	text := "name=John age=30 city=New York"
	re := regexp.MustCompile(`(?P<key>\w+)=(?P<val>\w+)`)

	matches := re.FindAllStringSubmatch(text, -1)
	for _, match := range matches {
		fmt.Printf("Key: %s, Value: %s\n", match[1], match[2])
	}

	// Output:
	// Key: name, Value: John
	// Key: age, Value: 30
	// Key: city, Value: New
}

func ExampleCompiledRoute_regex_disjunct() {

	text := "text1234"
	re := regexp.MustCompile(`(?P<text>[a-z]+)|(?P<num>[0-9]+)`)

	matches := re.FindAllStringSubmatch(text, -1)
	for _, match := range matches {
		fmt.Printf("Text: %s, Number: %s.\n", match[1], match[2])
	}

	// Output:
	// Text: text, Number: .
	// Text: , Number: 1234.
}
