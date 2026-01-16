package route

import (
	"fmt"
	"strings"

	"go.resystems.io/eddt/internal/relate"
	"go.resystems.io/eddt/internal/relate/relationset"
)

// execute runs the instructions in a pre-compiled routing rule against the given subject.
//
// The output is a set of egress subjects computed by the routing expansion.
func (r *CompiledRoute) execute(subject string, relations RelationSetSource) ([]string, error) {

	// expansion result set
	expansion := make([]*strings.Builder, 0, 8)

	// start with a single empty subject
	expansion = append(expansion, new(strings.Builder))

	// tokenised the split input string
	var tokenised []string

	// relation sets
	relsets_keys := make([]*strings.Builder, 0, 8)
	relsets := make([]*relationset.RelationSet, 0, 8)

	append_expansion := func(add string) {
		for i := range expansion {
			expansion[i].WriteString(add)
		}
	}

	extend_relset_keys := func(idx int) {
		if idx >= len(relsets_keys) {
			extended := make([]*strings.Builder, idx+1)
			copy(extended, relsets_keys)
			extended[idx] = new(strings.Builder)
			relsets_keys = extended
		}
	}

	for _, i := range r.Instructions {
		switch i.Op {
		case Tokenise:
			// split the input subject into tokens and populate the token set
			tokenised = strings.Split(subject, ".")
		case FetchRelSet:
			// fetch a relation-set buffer based on a referenced computed key
			k := relsets_keys[i.Rel].String()
			s, ok := relations.Get(k)
			if !ok {
				s = relate.EMPTY_REPLATIONSET
			}
			// store the fetched set
			if i.Rel >= len(relsets) {
				extended := make([]*relationset.RelationSet, i.Rel+1)
				copy(extended, relsets)
				relsets = extended
			}
			relsets[i.Rel] = s
		case AppendRelLiteral:
			// append the referenced string from the literal set to a relation key
			extend_relset_keys(i.Rel)
			relsets_keys[i.Rel].WriteString(r.Literals[i.Idx]) // panic if out of range
		case AppendRelToken:
			// append the referenced string from the token set to a relation key
			extend_relset_keys(i.Rel)
			if i.Idx >= len(tokenised) {
				return nil, fmt.Errorf("Token index [%d] out of range: <%s>", i.Idx, subject)
			}
			relsets_keys[i.Rel].WriteString(tokenised[i.Idx])
		case AppendEgressLiteral:
			// append the referenced string from the literal set to the each subject in the egress working batch
			append_expansion(r.Literals[i.Idx])
		case AppendEgressToken:
			// append the referenced string from the token set to the each subject in the egress working batch
			if i.Idx >= len(tokenised) {
				return nil, fmt.Errorf("Token index [%d] out of range: <%s>", i.Idx, subject)
			}
			append_expansion(tokenised[i.Idx])
		case AppendEgressRelations:
			// append each relation-set entry, as a string, from the referenced fetched relation-set to each subject
			// in the egress working batch (i.e. constructing a cross-product)
			if i.Rel > len(relsets) {
				panic(fmt.Errorf("Reference to unknown relation set [%d]", i.Rel))
			}
			s := relsets[i.Rel]
			if s == nil {
				panic(fmt.Errorf("nil set fetched from the source <%d:%s>", i.Rel, relsets_keys[i.Rel].String()))
			}
			sl := s.RLength()
			if sl > 0 {
				crossprod := make([]*strings.Builder, 0, len(expansion)*sl)
				rv := new(relationset.Relation)
				for si := range sl {
					for _, v := range expansion {
						rvok := s.R(rv, si)
						if rvok {
							c := new(strings.Builder)
							c.WriteString(v.String())
							c.Write(rv.Di())
							crossprod = append(crossprod, c)
						}
					}
				}
				expansion = crossprod
			}

		default:
			panic("Unsupported routing instruction.")
		}
	}

	text := make([]string, len(expansion))
	for i, e := range expansion {
		text[i] = e.String()
	}

	return text, nil
}

// execute runs the instructions in a pre-compiled routing rule against the given subject.
//
// The output is a set of egress subjects computed by the routing expansion.
func (r *CompiledRoute) execute_concat(subject string, relations RelationSetSource) ([]string, error) {

	// expansion result set
	expansion := make([]string, 0, 8)

	// start with a single empty subject
	expansion = append(expansion, "")

	// tokenised the split input string
	var tokenised []string

	// relation sets
	relsets_keys := make([]string, 0, 8)
	relsets := make([]*relationset.RelationSet, 0, 8)

	append_expansion := func(add string) {
		for i := range expansion {
			expansion[i] = expansion[i] + add
		}
	}

	extend_relset_keys := func(idx int) {
		if idx >= len(relsets_keys) {
			extended := make([]string, idx+1)
			copy(extended, relsets_keys)
			extended[idx] = ""
			relsets_keys = extended
		}
	}

	for _, i := range r.Instructions {
		switch i.Op {
		case Tokenise:
			// split the input subject into tokens and populate the token set
			tokenised = strings.Split(subject, ".")
		case FetchRelSet:
			// fetch a relation-set buffer based on a referenced computed key
			k := relsets_keys[i.Rel]
			s, ok := relations.Get(k)
			if !ok {
				s = relate.EMPTY_REPLATIONSET
			}
			// store the fetched set
			if i.Rel >= len(relsets) {
				extended := make([]*relationset.RelationSet, i.Rel+1)
				copy(extended, relsets)
				relsets = extended
			}
			relsets[i.Rel] = s
		case AppendRelLiteral:
			// append the referenced string from the literal set to a relation key
			extend_relset_keys(i.Rel)
			relsets_keys[i.Rel] = relsets_keys[i.Rel] + r.Literals[i.Idx] // panic if out of range
		case AppendRelToken:
			// append the referenced string from the token set to a relation key
			extend_relset_keys(i.Rel)
			if i.Idx >= len(tokenised) {
				return nil, fmt.Errorf("Token index [%d] out of range: <%s>", i.Idx, subject)
			}
			relsets_keys[i.Rel] = relsets_keys[i.Rel] + tokenised[i.Idx]
		case AppendEgressLiteral:
			// append the referenced string from the literal set to the each subject in the egress working batch
			append_expansion(r.Literals[i.Idx])
		case AppendEgressToken:
			// append the referenced string from the token set to the each subject in the egress working batch
			if i.Idx >= len(tokenised) {
				return nil, fmt.Errorf("Token index [%d] out of range: <%s>", i.Idx, subject)
			}
			append_expansion(tokenised[i.Idx])
		case AppendEgressRelations:
			// append each relation-set entry, as a string, from the referenced fetched relation-set to each subject
			// in the egress working batch (i.e. constructing a cross-product)
			if i.Rel > len(relsets) {
				panic(fmt.Errorf("Reference to unknown relation set [%d]", i.Rel))
			}
			s := relsets[i.Rel]
			if s == nil {
				panic(fmt.Errorf("nil set fetched from the source <%d:%s>", i.Rel, relsets_keys[i.Rel]))
			}
			sl := s.RLength()
			if sl > 0 {
				crossprod := make([]string, 0, len(expansion)*sl)
				rv := new(relationset.Relation)
				for si := range sl {
					for _, v := range expansion {
						rvok := s.R(rv, si)
						if rvok {
							crossprod = append(crossprod, v+string(rv.Di()))
						}
					}
				}
				expansion = crossprod
			} else {
				// not expansion found - fill in a blank
				append_expansion("_")
			}

		default:
			panic("Unsupported routing instruction.")
		}
	}
	return expansion, nil
}
