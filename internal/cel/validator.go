package cel

import (
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
)

// AllowedOperators defines the only operators/functions allowed in CEL expressions.
var AllowedOperators = map[string]bool{
	// Comparison operators
	"_==_": true,
	"_!=_": true,
	"_<_":  true,
	"_<=_": true,
	"_>_":  true,
	"_>=_": true,
	// Logical operators
	"_&&_": true,
	"_||_": true,
	"!_":   true,
	// Field/index access
	"_[_]": true,
	"_._":  true,
	// Conditional
	"_?_:_": true,
	// Type checking
	"has": true,
	// String functions
	"contains":   true,
	"startsWith": true,
	"endsWith":   true,
	"matches":    true,
	// Membership
	"@in": true,
	// List functions
	"exists": true,
	"all":    true,
	"size":   true,
	"map":    true,
	"filter": true,
}

// Validator handles the validation of CEL expressions for ResourceIndexPolicy
type Validator struct {
	env      *cel.Env
	maxDepth int
}

// NewValidator creates a new CEL validator with the standard environment and specified max recursion depth
func NewValidator(maxDepth int) (*Validator, error) {
	env, err := cel.NewEnv(
		cel.Variable("metadata", cel.DynType),
		cel.Variable("spec", cel.DynType),
		cel.Variable("status", cel.DynType),
	)
	if err != nil {
		return nil, err
	}
	return &Validator{env: env, maxDepth: maxDepth}, nil
}

// Validate validates a single CEL expression against the configured environment
func (v *Validator) Validate(expression string) []string {
	var errs []string

	if expression == "" {
		return errs
	}

	// 1. Compile the expression (combines Parse and Check)
	ast, issues := v.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		errs = append(errs, issues.Err().Error())
		return errs
	}

	// 2. Validate output type
	if ast.OutputType() != cel.BoolType {
		errs = append(errs, "expression must evaluate to a boolean")
	}

	// 3. Validate operators
	if errMsg := v.validateOperators(ast); errMsg != "" {
		errs = append(errs, errMsg)
	}

	return errs
}

func (v *Validator) validateOperators(parsedAST *cel.Ast) string {
	return v.checkExpr(parsedAST.NativeRep().Expr(), 0)
}

func (v *Validator) checkExpr(e ast.Expr, depth int) string {
	if e == nil {
		return ""
	}
	if depth > v.maxDepth {
		return "expression complexity exceeds maximum depth"
	}

	switch e.Kind() {
	case ast.CallKind:
		c := e.AsCall()
		funcName := c.FunctionName()

		// Check function name
		// Skip internal CEL functions (e.g. macro expansions like @not_strictly_false)
		if strings.HasPrefix(funcName, "@") {
			// standard logic continues
		} else if !AllowedOperators[funcName] {
			// Special exception: Allow _+_ (concatenation) ONLY if at least one argument is a list.
			// This is required for standard macros like 'filter' and 'map' which expand to
			// list concatenation (acc + [elem]).
			isListConcat := false
			if funcName == "_+_" && len(c.Args()) == 2 {
				if c.Args()[0].Kind() == ast.ListKind || c.Args()[1].Kind() == ast.ListKind {
					isListConcat = true
				}
			}

			if !isListConcat {
				return "operator or function '" + funcName + "' is not allowed; checks limited to basic comparison and list/map logic"
			}
		}
		// Recursively check target
		if c.IsMemberFunction() {
			if err := v.checkExpr(c.Target(), depth+1); err != "" {
				return err
			}
		}
		// Recursively check args
		for _, arg := range c.Args() {
			if err := v.checkExpr(arg, depth+1); err != "" {
				return err
			}
		}
	case ast.SelectKind:
		return v.checkExpr(e.AsSelect().Operand(), depth+1)
	case ast.ListKind:
		for _, elem := range e.AsList().Elements() {
			if err := v.checkExpr(elem, depth+1); err != "" {
				return err
			}
		}
	case ast.MapKind:
		for _, entry := range e.AsMap().Entries() {
			mapEntry := entry.AsMapEntry()
			if err := v.checkExpr(mapEntry.Key(), depth+1); err != "" {
				return err
			}
			if err := v.checkExpr(mapEntry.Value(), depth+1); err != "" {
				return err
			}
		}
	case ast.StructKind:
		for _, field := range e.AsStruct().Fields() {
			structField := field.AsStructField()
			if err := v.checkExpr(structField.Value(), depth+1); err != "" {
				return err
			}
		}
	case ast.ComprehensionKind:
		comp := e.AsComprehension()
		// Check all parts
		if err := v.checkExpr(comp.IterRange(), depth+1); err != "" {
			return err
		}
		if err := v.checkExpr(comp.AccuInit(), depth+1); err != "" {
			return err
		}
		if err := v.checkExpr(comp.LoopCondition(), depth+1); err != "" {
			return err
		}
		if err := v.checkExpr(comp.LoopStep(), depth+1); err != "" {
			return err
		}
		if err := v.checkExpr(comp.Result(), depth+1); err != "" {
			return err
		}
	}
	return ""
}
