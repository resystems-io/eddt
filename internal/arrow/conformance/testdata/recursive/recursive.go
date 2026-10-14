// Package recursive holds a self-referential struct.
package recursive

type Node struct {
	Name     string
	Children []*Node
}
