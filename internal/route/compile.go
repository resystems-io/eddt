package route

import (
	"fmt"
	"regexp"
	"strconv"

	"go.resystems.io/eddt/contract"
)

type CompiledRoute struct {
	ID contract.RouteID

	// Instructions are executed in order, with later instructions potentially depending on the output of earlier ones.
	//
	// Note, instructions are recorded by value and not reference in order to improve cache coherence.
	Instructions []RouteInstruction

	// Literals includes a dictionary of the string literals extracted when parsing the rule.
	//
	// Note, for simplicity we use references to strings, but we might achieve getter cache coherence if all of the
	// literals are packed into single buffer and then sliced out.
	Literals []string
}

type RouteInstruction struct {
	Op  RouteOpCode
	Idx int // the index into the source set (literal, token, relations)
	Rel int // the relation-set key being built for relation operations
}

type RouteOpCode int

const (
	Unknown               RouteOpCode = iota
	Tokenise                          // split the input subject into tokens and populate the token set
	FetchRelSet                          // fetch a relation-set buffer based on a referenced computed key
	AppendRelLiteral                  // append the referenced string from the literal set to a relation key
	AppendRelToken                    // append the referenced string from the token set to a relation key
	AppendEgressLiteral               // append the referenced string from the literal set to the each subject in the egress working batch
	AppendEgressToken                 // append the referenced string from the token set to the each subject in the egress working batch
	AppendEgressRelations             // append each relation-set entry, as a string, from the referenced fetched relation-set to each subject in the egress working batch (i.e. constructing a cross-product)
)

// -- implementations

// see: https://bnfplayground.pauliankline.com
// see: https://github.com/alecthomas/participle

// The following regex is used to split transformation subjects in accordance with the EBNF:
//
// ```ebnf
// <transform> ::= (<element>)*
// <element> ::= <literal> | <ref>
// <literal> ::= ([a-z] | [A-Z] | [0-9] | "-" | "_" | "." )
// <ref> ::= "${" <id> "}"
// <id> ::= <idx> | <name>
// <idx> ::= ([0-9])+
// <name> ::= [a-z] ([a-z] | [0-9] | "-")*
// ```
const TOKENISE_TRANSFORM_SUBJECT_REGEX = "(?P<literal>[a-zA-Z0-9_.-]+)|(?P<ref>\\$\\{(?P<id>(?P<name>[a-z]+)|(?P<idx>[0-9]+))\\})"

const (

	// Note, we record the mapping between named groups and numbered groups and check the mapping in a test.
	// This ensure that future changes to the regex will remained aligned with the group numbering.

	TRANSFORM_GROUP_LITERAL_ID = "literal"
	TRANSFORM_GROUP_NAME_ID    = "name"
	TRANSFORM_GROUP_IDX_ID     = "idx"

	TRANSFORM_GROUP_LITERAL = 1
	TRANSFORM_GROUP_NAME    = 4
	TRANSFORM_GROUP_IDX     = 5
)

// The following regex is used to split relation-set subjects in accordance with the EBNF:
//
// ```ebnf
// <relset> ::= (<element>)*
// <element> ::= <literal> | <ref>
// <literal> ::= ([a-z] | [A-Z] | [0-9] | "-" | "_" | "." )
// <ref> ::= "${" <idx> "}"
// <idx> ::= ([0-9])+
// ```
const TOKENISE_RELSET_SUBJECT_REGEX = "(?P<literal>[a-zA-Z0-9_.-]+)|(?P<ref>\\$\\{(?P<idx>[0-9]+)\\})"

const (

	// Note, we record the mapping between named groups and numbered groups and check the mapping in a test.
	// This ensure that future changes to the regex will remained aligned with the group numbering.

	RELSET_GROUP_LITERAL_ID = "literal"
	RELSET_GROUP_IDX_ID     = "idx"

	RELSET_GROUP_LITERAL = 1
	RELSET_GROUP_IDX     = 3
)

// Compile produces route expansion instructions that can be reused and executed during routing.
func Compile(r *contract.Route) (*CompiledRoute, error) {

	compiled := &CompiledRoute{
		ID:           r.ID,
		Instructions: make([]RouteInstruction, 0, 8),
		Literals:     make([]string, 0, 8),
	}

	literals := make(map[string]int) // reuse prior literals where possible
	keys := make(map[string]int)     // reuse prior named relations where possible

	// Note, literals are referenced by the append operations so the literal dictionary can not be reordered without
	// updating the instructions.

	// emit a generic 'tokenise' op code in order to populate the tokens needed by subsequent operations
	compiled.Instructions = append(compiled.Instructions, RouteInstruction{Tokenise, 0, 0})

	// split the transform mapping subject into sections by finding literal regions and ${id} elements.
	re := regexp.MustCompile(TOKENISE_TRANSFORM_SUBJECT_REGEX)
	matches := re.FindAllStringSubmatch(r.Transform, -1)
	for i, match := range matches {
		if len(match[TRANSFORM_GROUP_LITERAL]) > 0 {
			// handle literal
			v := match[TRANSFORM_GROUP_LITERAL]

			// update the literal dictionary and obtain the index
			lit := record_literal(compiled, literals, v)

			// emit an append egress literal operation
			compiled.Instructions = append(compiled.Instructions, RouteInstruction{AppendEgressLiteral, int(lit), 0})

		} else if len(match[TRANSFORM_GROUP_NAME]) > 0 {
			// handle named reference
			v := match[TRANSFORM_GROUP_NAME]

			// note, there is no need to process the relation reference again if it is reused
			key, ok := keys[v]
			if !ok {
				// get a new unique key value
				key = len(keys)
				// each parsed name pushes parsing of the corresponding relation expansion rule
				err := compile_push_relation(r, compiled, literals, key, v)
				if err != nil {
					return nil, fmt.Errorf("failed to parse named relation reference [%d] <%s>: %w", i, v, err)
				}
				// update the key look-up
				keys[v] = key
			}

			// emit an append egress relations operation (triggering a cross-product expansion using fetched sets)
			compiled.Instructions = append(compiled.Instructions, RouteInstruction{AppendEgressRelations, int(key), key})

		} else if len(match[TRANSFORM_GROUP_IDX]) > 0 {
			// handle token reference
			v := match[TRANSFORM_GROUP_IDX]

			// parse the index
			tok, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse token reference <%s>: %w", v, err)
			}

			// emit an append egress token op
			compiled.Instructions = append(compiled.Instructions, RouteInstruction{AppendEgressToken, int(tok), 0})
		}
	}

	return compiled, nil
}

func record_literal(compiled *CompiledRoute, literals map[string]int, v string) int {
	lit, ok := literals[v]
	if !ok {
		lit = len(compiled.Literals)
		compiled.Literals = append(compiled.Literals, v)
		literals[v] = lit
	}
	return lit
}

func compile_push_relation(r *contract.Route, compiled *CompiledRoute, literals map[string]int, key int, name string) error {

	// fetch the relation mapping
	relset, ok := r.References[name]
	if !ok {
		return fmt.Errorf("no relation-set mapping found for <%s>", name)
	}

	// when a relation expansion rule is pushed into the parsing pipeline the rule is fetched from the map and assigned
	// an internal index and then parsed into literals and token indices.
	re := regexp.MustCompile(TOKENISE_RELSET_SUBJECT_REGEX)
	matches := re.FindAllStringSubmatch(relset, -1)
	for _, match := range matches {
		if len(match[RELSET_GROUP_LITERAL]) > 0 {
			// handle literal
			v := match[RELSET_GROUP_LITERAL]

			// update the literal dictionary and obtain the index
			lit := record_literal(compiled, literals, v)

			// emit an append rel key literal operation
			compiled.Instructions = append(compiled.Instructions, RouteInstruction{AppendRelLiteral, int(lit), key})

		} else if len(match[RELSET_GROUP_IDX]) > 0 {
			// handle token reference
			v := match[RELSET_GROUP_IDX]

			// parse the index
			tok, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return fmt.Errorf("failed to parse token reference <%s>: %w", v, err)
			}

			// emit an append rel key token operation
			compiled.Instructions = append(compiled.Instructions, RouteInstruction{AppendRelToken, int(tok), key})
		}
	}

	// fetch set operations are emitted once the relation subject has been constructed via previous operations
	compiled.Instructions = append(compiled.Instructions, RouteInstruction{FetchRelSet, int(key), key})

	return nil
}
