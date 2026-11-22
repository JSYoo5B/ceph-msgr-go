package main

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// part is one part of a file name an integration test writes: literal text,
// or a value the guard cannot enumerate, any text ('*') or the decimal number
// of an integer verb ('d').
type part struct {
	text string
	hole byte
}

// shape is a file name as its parts; without a hole it is one exact name.
type shape []part

func (s shape) exact() bool {
	return !slices.ContainsFunc(s, func(p part) bool { return p.hole != 0 })
}

// String is the name, with "*" for each hole.
func (s shape) String() string {
	var b strings.Builder
	for _, p := range s {
		if p.hole != 0 {
			b.WriteByte('*')
		} else {
			b.WriteString(p.text)
		}
	}
	return b.String()
}

func (s shape) key() string {
	var b strings.Builder
	for _, p := range s {
		if p.hole != 0 {
			b.WriteByte(0)
			b.WriteByte(p.hole)
		} else {
			b.WriteString(p.text)
		}
	}
	return b.String()
}

func concat(a, b shape) shape {
	out := slices.Clone(a)
	for _, p := range b {
		if n := len(out); n > 0 && p.hole == 0 && out[n-1].hole == 0 {
			out[n-1].text += p.text
		} else if p.hole != 0 || p.text != "" {
			out = append(out, p)
		}
	}
	return out
}

func cross(a, b []shape) []shape {
	if len(a)*len(b) > 4096 {
		return anyName
	}
	var out []shape
	for _, x := range a {
		for _, y := range b {
			out = append(out, concat(x, y))
		}
	}
	return dedupe(out)
}

func dedupe(shapes []shape) []shape {
	seen := map[string]bool{}
	var out []shape
	for _, s := range shapes {
		if !seen[s.key()] {
			seen[s.key()] = true
			out = append(out, s)
		}
	}
	if len(out) > 4096 {
		return anyName
	}
	return out
}

var anyName = []shape{{{hole: '*'}}}

// plainControlInput is the shape of a control input the fixture reads under
// a plain name, which a test passes with its request instead
// (integration/control_input_test.go).
var plainControlInput = regexp.MustCompile(`(-input|-manifest)\.json$`)

// mayBeReceipt reports whether a name of the shape can be a receipt: a
// ".json" name that is no control input. A name ending in a digit hole or in
// literal text that cannot complete ".json" cannot be one.
func (s shape) mayBeReceipt() bool {
	if s.exact() {
		name := s.String()
		return strings.HasSuffix(name, ".json") && !plainControlInput.MatchString(name)
	}
	last := s[len(s)-1]
	switch {
	case last.hole == 'd':
		return false
	case last.hole != 0:
		return true
	case strings.HasSuffix(last.text, ".json"):
		return !plainControlInput.MatchString(last.text)
	default:
		return strings.HasSuffix(".json", last.text) && s[len(s)-2].hole != 'd'
	}
}

// zeroValue stands for the zero value of a variable or field declared
// without one: "" as a name, a literal without elements as a struct.
var zeroValue = &ast.CompositeLit{}

// assignment is the index-th left-hand side of an assignment or range
// statement.
type assignment struct {
	stmt  ast.Node
	index int
}

// readOnlyPackages are packages whose functions do not change what a pointer
// argument points to: fmt except its Scan functions, and encoding/json only
// by Marshal and MarshalIndent.
var readOnlyPackages = map[string]bool{"fmt": true, "testing": true, "log": true}

// resolver enumerates the values an expression of the type-checked
// integration tests can hold. It follows every variable through its
// declaration and every assignment, a parameter or receiver through the
// arguments of every call of its function or method, a call of a function of
// these files through its results, a field through the keyed or positional
// value of each struct literal its operand can be and every assignment to
// it, an element through the slice, array and map literals and appends it
// comes from, concatenation, constants and fmt.Sprintf %s and %d. Anything
// else is a hole: a value of another package or of an interface call, a
// parameter of a function used as a value or not called here, and a
// variable or field whose address is taken, that is changed in place (also
// through an alias of its slice, map or pointer), or that a pointer, slice
// or map passed to another package can change.
type resolver struct {
	info      *types.Info
	parent    map[ast.Node]ast.Node
	defs      map[types.Object]*ast.Ident
	funcDecls map[types.Object]*ast.FuncDecl
	assigns   map[types.Object][]assignment
	opaque    map[types.Object]bool
	calls     map[types.Object][]*ast.CallExpr
	escaped   map[types.Object]bool
	mutated   map[string]bool
	active    map[ast.Node]bool
}

func newResolver(files []*ast.File, info *types.Info) *resolver {
	r := &resolver{info: info, parent: map[ast.Node]ast.Node{}, defs: map[types.Object]*ast.Ident{},
		funcDecls: map[types.Object]*ast.FuncDecl{}, assigns: map[types.Object][]assignment{}, opaque: map[types.Object]bool{},
		calls: map[types.Object][]*ast.CallExpr{}, escaped: map[types.Object]bool{}, mutated: map[string]bool{}, active: map[ast.Node]bool{}}
	for id, obj := range info.Defs {
		if obj != nil {
			r.defs[obj] = id
		}
	}
	for _, f := range files {
		var stack []ast.Node
		ast.Inspect(f, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			if len(stack) > 0 {
				r.parent[n] = stack[len(stack)-1]
			}
			stack = append(stack, n)
			if d, ok := n.(*ast.FuncDecl); ok && info.Defs[d.Name] != nil {
				r.funcDecls[info.Defs[d.Name]] = d
			}
			return true
		})
	}
	var calls []*ast.CallExpr
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			r.index(n)
			if call, ok := n.(*ast.CallExpr); ok {
				calls = append(calls, call)
			}
			return true
		})
	}
	for _, call := range calls {
		r.external(call)
	}
	return r
}

func (r *resolver) index(n ast.Node) {
	switch n := n.(type) {
	case *ast.AssignStmt:
		for i, lhs := range n.Lhs {
			r.assign(lhs, assignment{n, i})
		}
	case *ast.RangeStmt:
		if n.Tok == token.ASSIGN {
			for i, e := range []ast.Expr{n.Key, n.Value} {
				if e != nil {
					r.assign(e, assignment{n, i})
				}
			}
		}
	case *ast.IncDecStmt:
		r.mutate(n.X)
	case *ast.UnaryExpr:
		if n.Op == token.AND {
			r.mutate(n.X)
		}
	case *ast.CallExpr:
		if callee := r.callee(n); callee != nil {
			r.calls[callee] = append(r.calls[callee], n)
		}
	case *ast.Ident:
		obj := r.info.Uses[n]
		if !r.callable(obj) {
			return
		}
		// A function, method or function variable used other than by
		// calling it, or assigning it, can be called unseen.
		switch p := r.parent[n].(type) {
		case *ast.CallExpr:
			if p.Fun == n {
				return
			}
		case *ast.SelectorExpr:
			if call, ok := r.parent[p].(*ast.CallExpr); ok && p.Sel == n && call.Fun == p {
				return
			}
		case *ast.AssignStmt:
			if slices.Contains(p.Lhs, ast.Expr(n)) {
				return
			}
		}
		r.escaped[obj] = true
	}
}

func (r *resolver) callable(obj types.Object) bool {
	switch obj := obj.(type) {
	case *types.Func:
		return true
	case *types.Var:
		_, ok := obj.Type().Underlying().(*types.Signature)
		return ok && !obj.IsField()
	}
	return false
}

// callee is the function, method or function variable a call calls.
func (r *resolver) callee(call *ast.CallExpr) types.Object {
	var obj types.Object
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		obj = r.info.Uses[fun]
	case *ast.SelectorExpr:
		if sel := r.info.Selections[fun]; sel != nil {
			if sel.Kind() != types.MethodVal {
				return nil
			}
			obj = sel.Obj()
		} else {
			obj = r.info.Uses[fun.Sel]
		}
	}
	if fn, ok := obj.(*types.Func); ok {
		return fn.Origin()
	}
	return obj
}

func (r *resolver) assign(lhs ast.Expr, a assignment) {
	switch lhs := ast.Unparen(lhs).(type) {
	case *ast.Ident:
		if r.info.Defs[lhs] == nil {
			if obj := r.info.Uses[lhs]; obj != nil {
				r.assigns[obj] = append(r.assigns[obj], a)
			}
		}
	case *ast.SelectorExpr:
		if obj := r.fieldOf(lhs); obj != nil {
			r.assigns[obj] = append(r.assigns[obj], a)
		}
	case *ast.IndexExpr, *ast.StarExpr:
		// An element or pointee changed in place is changed for every alias.
		var x ast.Expr
		if index, ok := lhs.(*ast.IndexExpr); ok {
			x = index.X
		} else {
			x = lhs.(*ast.StarExpr).X
		}
		r.changes(r.info.TypeOf(x))
		r.mutate(lhs)
	default:
		r.mutate(lhs)
	}
}

// changes records that the elements or pointees of values of type t can be
// changed in place; changed reports it. Types are told apart by their
// names, so two local types of one name count as one.
func (r *resolver) changes(t types.Type) {
	if t != nil {
		r.mutated[types.TypeString(t, nil)] = true
	}
}

func (r *resolver) changed(t types.Type) bool {
	return t != nil && r.mutated[types.TypeString(t, nil)]
}

func (r *resolver) fieldOf(sel *ast.SelectorExpr) types.Object {
	if s := r.info.Selections[sel]; s != nil && s.Kind() == types.FieldVal {
		return s.Obj()
	}
	return nil
}

// mutate marks a variable or field that is changed in place or through its
// address.
func (r *resolver) mutate(e ast.Expr) {
	switch e := ast.Unparen(e).(type) {
	case *ast.Ident:
		if obj := r.info.Uses[e]; obj != nil {
			r.opaque[obj] = true
		}
	case *ast.SelectorExpr:
		if obj := r.fieldOf(e); obj != nil {
			r.opaque[obj] = true
			r.mutate(e.X)
		}
	case *ast.IndexExpr:
		r.mutate(e.X)
	case *ast.StarExpr:
		r.mutate(e.X)
	}
}

// readOnly reports whether a function of another package does not change
// what its arguments point to.
func readOnly(fn *types.Func) bool {
	if fn.Pkg() == nil {
		return false
	}
	switch fn.Pkg().Path() {
	case "fmt":
		return !strings.Contains(fn.Name(), "Scan")
	case "encoding/json":
		return strings.HasPrefix(fn.Name(), "Marshal")
	}
	return readOnlyPackages[fn.Pkg().Path()]
}

// own reports whether every function a function expression can be is of
// these files, or does not change what its arguments point to.
func (r *resolver) own(e ast.Expr) bool {
	switch e := ast.Unparen(e).(type) {
	case *ast.FuncLit:
		return true
	case *ast.Ident:
		switch obj := r.info.Uses[e].(type) {
		case *types.Func:
			return r.funcDecls[obj] != nil || readOnly(obj)
		case *types.Nil:
			return true
		case *types.Var:
			if !r.enter(e) {
				return false
			}
			defer delete(r.active, e)
			es, ok := r.sources(obj)
			return ok && !slices.ContainsFunc(es, func(e ast.Expr) bool { return e == zeroValue || !r.own(e) })
		}
	case *ast.SelectorExpr:
		s := r.info.Selections[e]
		if s == nil || s.Kind() == types.MethodVal {
			fn, ok := r.info.Uses[e.Sel].(*types.Func)
			return ok && (r.funcDecls[fn] != nil || readOnly(fn))
		}
		if s.Kind() == types.FieldVal && r.enter(e) {
			defer delete(r.active, e)
			es, ok := r.field(e)
			return ok && !slices.ContainsFunc(es, func(e ast.Expr) bool { return e == zeroValue || !r.own(e) })
		}
	}
	return false
}

// external marks the fields that a call of another package can change
// through a pointer, slice or map argument or receiver.
func (r *resolver) external(call *ast.CallExpr) {
	if tv, found := r.info.Types[call.Fun]; found && (tv.IsType() || tv.IsBuiltin() && r.builtin(call) != "copy") {
		return
	}
	if r.builtin(call) != "copy" && r.own(call.Fun) {
		return
	}
	args := slices.Clone(call.Args)
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok && r.info.Selections[sel] != nil && r.info.Selections[sel].Kind() == types.MethodVal {
		args = append(args, sel.X)
	}
	for _, arg := range args {
		r.reachable(r.info.TypeOf(arg), false, map[types.Type]bool{})
	}
}

func (r *resolver) builtin(call *ast.CallExpr) string {
	if id, ok := ast.Unparen(call.Fun).(*ast.Ident); ok {
		if b, ok := r.info.Uses[id].(*types.Builtin); ok {
			return b.Name()
		}
	}
	return ""
}

// reachable marks every field another package can change through a value
// of type t: behind a pointer, slice or map, and the fields of a struct so
// reached.
func (r *resolver) reachable(t types.Type, behind bool, seen map[types.Type]bool) {
	if t == nil || seen[t] {
		return
	}
	seen[t] = true
	if behind {
		r.changes(t)
	}
	switch u := t.Underlying().(type) {
	case *types.Pointer:
		r.changes(t)
		r.reachable(u.Elem(), true, seen)
	case *types.Slice:
		r.changes(t)
		r.reachable(u.Elem(), true, seen)
	case *types.Map:
		r.changes(t)
		r.reachable(u.Elem(), true, seen)
	case *types.Array:
		r.reachable(u.Elem(), behind, seen)
	case *types.Struct:
		for f := range u.Fields() {
			if behind {
				r.opaque[f] = true
			}
			r.reachable(f.Type(), behind, seen)
		}
	}
}

func (r *resolver) enter(n ast.Node) bool {
	if r.active[n] || len(r.active) > 256 {
		return false
	}
	r.active[n] = true
	return true
}

// sources returns every expression whose value the variable can hold.
func (r *resolver) sources(obj types.Object) ([]ast.Expr, bool) {
	if _, ok := obj.(*types.Nil); ok {
		return nil, true
	}
	v, ok := obj.(*types.Var)
	if !ok || r.opaque[obj] {
		return nil, false
	}
	def := r.defs[obj]
	if def == nil {
		return nil, false // of another package
	}
	var out []ast.Expr
	ok = false
	switch d := r.parent[def].(type) {
	case *ast.ValueSpec:
		i := slices.Index(d.Names, def)
		switch {
		case len(d.Values) == 0:
			out, ok = []ast.Expr{zeroValue}, true
		case len(d.Values) == len(d.Names):
			out, ok = []ast.Expr{d.Values[i]}, true
		case len(d.Values) == 1:
			if call, isCall := ast.Unparen(d.Values[0]).(*ast.CallExpr); isCall {
				out, ok = r.returns(call, i)
			}
		}
	case *ast.AssignStmt:
		out, ok = r.assigned(assignment{d, slices.Index(d.Lhs, ast.Expr(def))})
	case *ast.RangeStmt:
		if d.Value == def {
			out, ok = r.elements(d.X)
		}
	case *ast.Field:
		if !v.IsField() {
			out, ok = r.arguments(def, d)
		}
	}
	if !ok {
		return nil, false
	}
	for _, a := range r.assigns[obj] {
		es, ok := r.assigned(a)
		if !ok {
			return nil, false
		}
		out = append(out, es...)
	}
	return out, true
}

// assigned returns what an assignment gives its index-th left-hand side.
func (r *resolver) assigned(a assignment) ([]ast.Expr, bool) {
	switch s := a.stmt.(type) {
	case *ast.RangeStmt:
		if a.index == 1 {
			return r.elements(s.X)
		}
	case *ast.AssignStmt:
		if a.index < 0 || s.Tok != token.ASSIGN && s.Tok != token.DEFINE {
			return nil, false
		}
		if len(s.Lhs) == len(s.Rhs) {
			return []ast.Expr{s.Rhs[a.index]}, true
		}
		if len(s.Rhs) == 1 {
			switch e := ast.Unparen(s.Rhs[0]).(type) {
			case *ast.IndexExpr:
				if a.index == 0 {
					return []ast.Expr{e}, true
				}
			case *ast.CallExpr:
				return r.returns(e, a.index)
			}
		}
	}
	return nil, false
}

// function is the function, method or function variable that declares a
// signature.
func (r *resolver) function(ft *ast.FuncType) types.Object {
	switch fn := r.parent[ft].(type) {
	case *ast.FuncDecl:
		return r.info.Defs[fn.Name]
	case *ast.FuncLit:
		var id *ast.Ident
		switch p := r.parent[fn].(type) {
		case *ast.AssignStmt:
			if i := slices.Index(p.Rhs, ast.Expr(fn)); i >= 0 && len(p.Lhs) == len(p.Rhs) {
				id, _ = p.Lhs[i].(*ast.Ident)
			}
		case *ast.ValueSpec:
			if i := slices.Index(p.Values, ast.Expr(fn)); i >= 0 && len(p.Values) == len(p.Names) {
				id = p.Names[i]
			}
		}
		if id != nil {
			return r.info.ObjectOf(id)
		}
	}
	return nil
}

// arguments returns the argument of every call for a parameter, or the
// operand of every call for a receiver. A function not called here, or used
// as a value, gives no values it can be proved to receive.
func (r *resolver) arguments(id *ast.Ident, field *ast.Field) ([]ast.Expr, bool) {
	list, _ := r.parent[field].(*ast.FieldList)
	if list == nil {
		return nil, false
	}
	if _, ok := field.Type.(*ast.Ellipsis); ok {
		return nil, false
	}
	index, count := -1, 0
	for _, f := range list.List {
		for _, n := range f.Names {
			if n == id {
				index = count
			}
			count++
		}
		if len(f.Names) == 0 {
			count++
		}
	}
	var fn types.Object
	receiver := false
	switch owner := r.parent[list].(type) {
	case *ast.FuncDecl:
		fn, receiver = r.info.Defs[owner.Name], true
	case *ast.FuncType:
		if owner.Params != list {
			return nil, false // a named result
		}
		fn = r.function(owner)
	}
	if fn == nil || r.escaped[fn] || len(r.calls[fn]) == 0 || index < 0 {
		return nil, false
	}
	var out []ast.Expr
	for _, call := range r.calls[fn] {
		if receiver {
			sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
			if !ok || len(r.info.Selections[sel].Index()) != 1 {
				return nil, false // a promoted method
			}
			out = append(out, sel.X)
			continue
		}
		if call.Ellipsis.IsValid() || index >= len(call.Args) {
			return nil, false
		}
		out = append(out, call.Args[index])
	}
	return out, true
}

// returns returns the index-th result of every return statement of a called
// function, method or function literal of these files.
func (r *resolver) returns(call *ast.CallExpr, index int) ([]ast.Expr, bool) {
	var bodies []*ast.BlockStmt
	switch fn := r.callee(call).(type) {
	case *types.Func:
		d := r.funcDecls[fn]
		if d == nil || d.Body == nil {
			return nil, false
		}
		bodies = append(bodies, d.Body)
	case *types.Var:
		es, ok := r.sources(fn)
		if !ok {
			return nil, false
		}
		for _, e := range es {
			lit, ok := ast.Unparen(e).(*ast.FuncLit)
			if !ok {
				return nil, false
			}
			bodies = append(bodies, lit.Body)
		}
	default:
		return nil, false
	}
	var out []ast.Expr
	ok := true
	for _, body := range bodies {
		ast.Inspect(body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.FuncLit:
				return false
			case *ast.ReturnStmt:
				if len(n.Results) == 1 && index == 0 || len(n.Results) > 1 && index < len(n.Results) {
					out = append(out, n.Results[index])
				} else {
					ok = false
				}
			}
			return true
		})
	}
	return out, ok
}

// lits returns every composite literal the expression can be.
func (r *resolver) lits(e ast.Expr) ([]*ast.CompositeLit, bool) {
	if e == zeroValue {
		return []*ast.CompositeLit{zeroValue}, true
	}
	if !r.enter(e) {
		return nil, false
	}
	defer delete(r.active, e)
	if t := r.info.TypeOf(e); t != nil {
		if _, isPointer := t.Underlying().(*types.Pointer); isPointer && r.changed(t) {
			return nil, false
		}
	}
	var es []ast.Expr
	ok := false
	switch e := e.(type) {
	case *ast.CompositeLit:
		return []*ast.CompositeLit{e}, true
	case *ast.UnaryExpr:
		if e.Op == token.AND {
			return r.lits(e.X)
		}
	case *ast.ParenExpr:
		return r.lits(e.X)
	case *ast.StarExpr:
		return r.lits(e.X)
	case *ast.Ident:
		if obj := r.info.Uses[e]; obj != nil {
			es, ok = r.sources(obj)
		}
	case *ast.SelectorExpr:
		es, ok = r.field(e)
	case *ast.IndexExpr:
		es, ok = r.elements(e.X)
	case *ast.CallExpr:
		es, ok = r.returns(e, 0)
	}
	if !ok {
		return nil, false
	}
	return r.litsOf(es)
}

func (r *resolver) litsOf(es []ast.Expr) ([]*ast.CompositeLit, bool) {
	var out []*ast.CompositeLit
	for _, e := range es {
		lits, ok := r.lits(e)
		if !ok {
			return nil, false
		}
		out = append(out, lits...)
	}
	return out, true
}

// elements returns every element a slice, array or map expression can hold:
// of its literals, appends and clones.
func (r *resolver) elements(e ast.Expr) ([]ast.Expr, bool) {
	if r.changed(r.info.TypeOf(e)) {
		return nil, false
	}
	if call, ok := ast.Unparen(e).(*ast.CallExpr); ok && len(call.Args) > 0 {
		if r.builtin(call) == "make" {
			return nil, true
		}
		clone := r.isFunc(call, "slices", "Clone")
		if clone || r.builtin(call) == "append" {
			out, ok := r.elements(call.Args[0])
			if !ok {
				return nil, false
			}
			if call.Ellipsis.IsValid() {
				more, ok := r.elements(call.Args[1])
				return append(out, more...), ok
			}
			if !clone {
				out = append(out, call.Args[1:]...)
			}
			return out, true
		}
	}
	if id, ok := ast.Unparen(e).(*ast.Ident); ok {
		obj := r.info.Uses[id]
		if obj == nil || !r.enter(id) {
			return nil, false
		}
		defer delete(r.active, id)
		es, ok := r.sources(obj)
		if !ok {
			return nil, false
		}
		var out []ast.Expr
		for _, e := range es {
			if e == zeroValue {
				continue // a nil slice or map
			}
			more, ok := r.elements(e)
			if !ok {
				return nil, false
			}
			out = append(out, more...)
		}
		return out, true
	}
	lits, ok := r.lits(e)
	if !ok {
		return nil, false
	}
	var out []ast.Expr
	for _, lit := range lits {
		if lit == zeroValue {
			continue
		}
		if _, isStruct := r.structOf(lit); isStruct {
			return nil, false
		}
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				elt = kv.Value
			}
			out = append(out, elt)
		}
	}
	return out, true
}

// field returns every value of the selected field: its keyed or positional
// value, or the zero value, in each struct literal the operand can be, and
// every assignment to the field. A promoted field is read through each
// embedded field on its path.
func (r *resolver) field(sel *ast.SelectorExpr) ([]ast.Expr, bool) {
	s := r.info.Selections[sel]
	if s == nil || s.Kind() != types.FieldVal {
		return nil, false
	}
	lits, ok := r.lits(sel.X)
	if !ok {
		return nil, false
	}
	var out []ast.Expr
	for step, index := range s.Index() {
		out = nil
		var f types.Object
		for _, lit := range lits {
			if lit == zeroValue {
				out = append(out, zeroValue)
				continue
			}
			st, _ := r.structOf(lit)
			if st == nil || index >= st.NumFields() {
				return nil, false
			}
			f = st.Field(index)
			value := ast.Expr(zeroValue)
			for i, elt := range lit.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.Ident); ok && r.info.Uses[key] == f {
						value = kv.Value
					}
				} else if i == index {
					value = elt
				}
			}
			out = append(out, value)
		}
		if f == nil && step == len(s.Index())-1 {
			f = s.Obj()
		}
		if f != nil {
			if r.opaque[f] {
				return nil, false
			}
			for _, a := range r.assigns[f] {
				if !r.mayAlias(lits, a) {
					continue
				}
				es, ok := r.assigned(a)
				if !ok {
					return nil, false
				}
				out = append(out, es...)
			}
		}
		if step < len(s.Index())-1 {
			if lits, ok = r.litsOf(out); !ok {
				return nil, false
			}
		}
	}
	return out, true
}

// mayAlias reports whether a field assignment can change one of lits: its
// operand can be one of them, or what it can be is not known.
func (r *resolver) mayAlias(lits []*ast.CompositeLit, a assignment) bool {
	var lhs ast.Expr
	switch s := a.stmt.(type) {
	case *ast.AssignStmt:
		lhs = s.Lhs[a.index]
	case *ast.RangeStmt:
		lhs = []ast.Expr{s.Key, s.Value}[a.index]
	}
	operand := ast.Unparen(lhs).(*ast.SelectorExpr).X
	others, ok := r.lits(operand)
	return !ok || slices.ContainsFunc(others, func(l *ast.CompositeLit) bool { return slices.Contains(lits, l) })
}

func (r *resolver) structOf(lit *ast.CompositeLit) (*types.Struct, bool) {
	t := r.info.TypeOf(lit)
	if t == nil {
		return nil, false
	}
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = p.Elem()
	}
	st, ok := t.Underlying().(*types.Struct)
	return st, ok
}

func (r *resolver) isString(e ast.Expr) bool {
	b, ok := r.info.TypeOf(e).Underlying().(*types.Basic)
	return ok && b.Info()&types.IsString != 0
}

// values returns every name the string expression can be.
func (r *resolver) values(e ast.Expr) []shape {
	if e == zeroValue {
		return []shape{{}}
	}
	if tv, ok := r.info.Types[e]; ok && tv.Value != nil {
		if tv.Value.Kind() == constant.String {
			return []shape{{{text: constant.StringVal(tv.Value)}}}
		}
		return anyName
	}
	if !r.enter(e) {
		return anyName
	}
	defer delete(r.active, e)
	var es []ast.Expr
	ok := false
	switch e := e.(type) {
	case *ast.ParenExpr:
		return r.values(e.X)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return anyName
		}
		return cross(r.values(e.X), r.values(e.Y))
	case *ast.CallExpr:
		if r.isFunc(e, "fmt", "Sprintf") {
			return r.sprintf(e)
		}
		if tv, found := r.info.Types[e.Fun]; found && tv.IsType() {
			if len(e.Args) == 1 && r.isString(e.Args[0]) {
				return r.values(e.Args[0]) // a conversion between string types
			}
			return anyName
		}
		es, ok = r.returns(e, 0)
	case *ast.Ident:
		if obj := r.info.Uses[e]; obj != nil {
			es, ok = r.sources(obj)
		}
	case *ast.SelectorExpr:
		es, ok = r.field(e)
	case *ast.IndexExpr:
		if !r.isString(e.X) {
			es, ok = r.elements(e.X)
		}
	}
	if !ok {
		return anyName
	}
	var out []shape
	for _, e := range es {
		out = append(out, r.values(e)...)
	}
	return dedupe(out)
}

// integers returns every value an integer expression can be, if they are
// constants.
func (r *resolver) integers(e ast.Expr) ([]int64, bool) {
	if e == zeroValue {
		return []int64{0}, true
	}
	if tv, ok := r.info.Types[e]; ok && tv.Value != nil {
		n, exact := constant.Int64Val(tv.Value)
		return []int64{n}, exact && tv.Value.Kind() == constant.Int
	}
	id, ok := ast.Unparen(e).(*ast.Ident)
	if !ok || !r.enter(id) {
		return nil, false
	}
	defer delete(r.active, id)
	obj := r.info.Uses[id]
	if obj == nil {
		return nil, false
	}
	es, ok := r.sources(obj)
	if !ok {
		return nil, false
	}
	var out []int64
	for _, e := range es {
		ns, ok := r.integers(e)
		if !ok {
			return nil, false
		}
		out = append(out, ns...)
	}
	return out, true
}

var formatVerb = regexp.MustCompile(`%[-+# 0-9.]*[a-zA-Z%]`)

// sprintf enumerates a fmt.Sprintf name: %s of a string and %d of an integer
// it can enumerate, a hole for any other argument or verb.
func (r *resolver) sprintf(call *ast.CallExpr) []shape {
	tv, ok := r.info.Types[call.Args[0]]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String || call.Ellipsis.IsValid() {
		return anyName
	}
	format := constant.StringVal(tv.Value)
	out, args, last := []shape{{}}, call.Args[1:], 0
	for _, m := range formatVerb.FindAllStringIndex(format, -1) {
		verb := format[m[0]:m[1]]
		out = cross(out, []shape{{{text: format[last:m[0]]}}})
		last = m[1]
		if verb == "%%" {
			out = cross(out, []shape{{{text: "%"}}})
			continue
		}
		if len(args) == 0 {
			return anyName
		}
		arg := args[0]
		args = args[1:]
		next := anyName
		switch verb[len(verb)-1] {
		case 's':
			if verb == "%s" && r.isString(arg) {
				next = r.values(arg)
			}
		case 'd':
			next = []shape{{{hole: 'd'}}}
			if ns, ok := r.integers(arg); ok {
				next = nil
				for _, n := range ns {
					next = append(next, shape{{text: fmt.Sprintf(verb, n)}})
				}
			}
		}
		out = cross(out, next)
	}
	if len(args) != 0 {
		return anyName
	}
	return cross(out, []shape{{{text: format[last:]}}})
}

func (r *resolver) isFunc(call *ast.CallExpr, pkg, name string) bool {
	fn, ok := r.callee(call).(*types.Func)
	return ok && fn.Pkg() != nil && fn.Pkg().Path() == pkg && fn.Name() == name
}

// baseNames returns every file name a path expression can name: the
// elements of filepath.Join after the directory (a name with "/" is in a
// subdirectory, which saveMetadata does not collect), through variables
// that hold one.
func (r *resolver) baseNames(e ast.Expr) []shape {
	switch e := ast.Unparen(e).(type) {
	case *ast.CallExpr:
		if r.isFunc(e, "path/filepath", "Join") && len(e.Args) > 1 && !e.Ellipsis.IsValid() {
			out := r.values(e.Args[1])
			for _, a := range e.Args[2:] {
				out = cross(cross(out, []shape{{{text: "/"}}}), r.values(a))
			}
			return out
		}
	case *ast.Ident:
		obj := r.info.Uses[e]
		if obj == nil || !r.enter(e) {
			return anyName
		}
		defer delete(r.active, e)
		es, ok := r.sources(obj)
		if !ok {
			return anyName
		}
		var out []shape
		for _, e := range es {
			out = append(out, r.baseNames(e)...)
		}
		return dedupe(out)
	}
	var out []shape
	for _, s := range r.values(e) {
		if s.exact() {
			s = shape{{text: filepath.Base(s.String())}}
		}
		out = append(out, s)
	}
	return out
}

// receiptWrite is one file write of an integration test: every name it can
// write, and where.
type receiptWrite struct {
	Names    []shape
	Position string
}

const modulePath = "github.com/JSYoo5B/ceph-msgr-go"

// receiptWrites returns every file write in files: the path of os.WriteFile,
// os.Create, os.OpenFile and testcluster.WritePublicEvidence, and the target
// of os.Rename and os.Link.
func receiptWrites(fset *token.FileSet, files []*ast.File, info *types.Info) []receiptWrite {
	r := newResolver(files, info)
	var writes []receiptWrite
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			arg := -1
			switch {
			case r.isFunc(call, "os", "WriteFile"), r.isFunc(call, "os", "Create"), r.isFunc(call, "os", "OpenFile"),
				r.isFunc(call, modulePath+"/internal/testcluster", "WritePublicEvidence"):
				arg = 0
			case r.isFunc(call, "os", "Rename"), r.isFunc(call, "os", "Link"):
				arg = 1
			}
			if arg >= 0 && arg < len(call.Args) {
				writes = append(writes, receiptWrite{Names: r.baseNames(call.Args[arg]), Position: fset.Position(call.Pos()).String()})
			}
			return true
		})
	}
	return writes
}

// sourceImporter type-checks the packages of this repository from source,
// and the standard library with std. Another package is empty: what a test
// takes from it is a hole.
type sourceImporter struct {
	fset     *token.FileSet
	root     string
	std      types.Importer
	packages map[string]*types.Package
}

func (s *sourceImporter) Import(importPath string) (*types.Package, error) {
	if p := s.packages[importPath]; p != nil {
		return p, nil
	}
	var p *types.Package
	switch {
	case strings.HasPrefix(importPath, modulePath+"/"):
		dir := filepath.Join(s.root, strings.TrimPrefix(importPath, modulePath+"/"))
		listed, err := build.Default.ImportDir(dir, 0)
		if err != nil {
			return nil, err
		}
		var files []*ast.File
		for _, name := range listed.GoFiles {
			f, err := parser.ParseFile(s.fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				return nil, err
			}
			files = append(files, f)
		}
		p, _ = (&types.Config{Importer: s, Error: func(error) {}}).Check(importPath, s.fset, files, nil)
	case !strings.Contains(strings.Split(importPath, "/")[0], "."):
		var err error
		if p, err = s.std.Import(importPath); err != nil {
			return nil, err
		}
	default:
		p = types.NewPackage(importPath, path.Base(importPath))
		p.MarkComplete()
	}
	s.packages[importPath] = p
	return p, nil
}

// typeCheck type-checks files as one package and returns what it records.
func typeCheck(fset *token.FileSet, files []*ast.File, imports *sourceImporter) (*types.Info, []error) {
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var errs []error
	_, _ = (&types.Config{Importer: imports, Error: func(err error) { errs = append(errs, err) }}).Check("integration_test", fset, files, info)
	return info, errs
}
