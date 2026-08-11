package source

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

const predicatePluginSourcePath = "pkg/scheduler/plugins/predicates/predicates.go"

// LoadPredicatePluginDefaults reads the predicates plugin from an already
// prepared Volcano worktree. It returns only defaults that can be linked
// unambiguously from a predicateEnable bool field, through args.GetBool, to a
// string-valued configuration-key constant.
func LoadPredicatePluginDefaults(worktree string) (map[string]bool, error) {
	root, err := schedulerSourceRoot(worktree)
	if err != nil {
		return nil, err
	}

	candidate, found, err := loadSchedulerSource(root, predicatePluginSourcePath)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("Volcano predicates plugin source %s is missing", predicatePluginSourcePath)
	}

	file, err := parser.ParseFile(
		token.NewFileSet(),
		predicatePluginSourcePath,
		candidate.content,
		parser.SkipObjectResolution,
	)
	if err != nil {
		return nil, fmt.Errorf("parse predicates plugin %s: %w", predicatePluginSourcePath, err)
	}

	booleanFields, err := predicateEnableBooleanFields(file)
	if err != nil {
		return nil, err
	}

	function, argumentsName, err := enablePredicateFunction(file)
	if err != nil {
		return nil, err
	}

	predicateName, literalEnd, defaults, err := predicateEnableLiteralDefaults(
		function,
		booleanFields,
	)
	if err != nil {
		return nil, err
	}

	constants := predicateStringConstants(file)

	return predicateGetBoolDefaults(
		function,
		argumentsName,
		predicateName,
		literalEnd,
		booleanFields,
		defaults,
		constants,
	)
}

func predicateEnableBooleanFields(file *ast.File) (map[string]bool, error) {
	var target *ast.StructType

	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}

		for _, specification := range general.Specs {
			typeSpec, ok := specification.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "predicateEnable" {
				continue
			}

			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return nil, fmt.Errorf(
					"predicateEnable in %s is not a struct",
					predicatePluginSourcePath,
				)
			}

			if target != nil {
				return nil, fmt.Errorf(
					"multiple predicateEnable declarations in %s",
					predicatePluginSourcePath,
				)
			}

			target = structure
		}
	}

	if target == nil {
		return nil, fmt.Errorf(
			"predicateEnable declaration is missing from %s",
			predicatePluginSourcePath,
		)
	}

	fields := make(map[string]bool)

	for _, field := range target.Fields.List {
		identifier, ok := field.Type.(*ast.Ident)
		if !ok || identifier.Name != "bool" {
			continue
		}

		if len(field.Names) != 1 {
			return nil, fmt.Errorf(
				"predicateEnable bool field in %s must have exactly one name",
				predicatePluginSourcePath,
			)
		}

		fields[field.Names[0].Name] = true
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf(
			"predicateEnable in %s has no bool fields",
			predicatePluginSourcePath,
		)
	}

	return fields, nil
}

func enablePredicateFunction(file *ast.File) (*ast.FuncDecl, string, error) {
	var target *ast.FuncDecl

	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "enablePredicate" {
			continue
		}

		if target != nil {
			return nil, "", fmt.Errorf(
				"multiple enablePredicate declarations in %s",
				predicatePluginSourcePath,
			)
		}

		target = function
	}

	if target == nil || target.Body == nil {
		return nil, "", fmt.Errorf(
			"enablePredicate declaration is missing from %s",
			predicatePluginSourcePath,
		)
	}

	if target.Type.Params == nil || len(target.Type.Params.List) != 1 {
		return nil, "", fmt.Errorf("enablePredicate must have one named Arguments parameter")
	}

	parameter := target.Type.Params.List[0]
	if len(parameter.Names) != 1 || !isArgumentsType(parameter.Type) {
		return nil, "", fmt.Errorf("enablePredicate must have one named Arguments parameter")
	}

	return target, parameter.Names[0].Name, nil
}

func isArgumentsType(expression ast.Expr) bool {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name == "Arguments"
	case *ast.SelectorExpr:
		return typed.Sel.Name == "Arguments"
	default:
		return false
	}
}

func predicateEnableLiteralDefaults(
	function *ast.FuncDecl,
	booleanFields map[string]bool,
) (string, token.Pos, map[string]bool, error) {
	predicateName := ""
	var literal *ast.CompositeLit

	for _, statement := range function.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
			continue
		}

		candidate, ok := assignment.Rhs[0].(*ast.CompositeLit)
		if !ok || !isPredicateEnableType(candidate.Type) {
			continue
		}

		if assignment.Tok != token.DEFINE && assignment.Tok != token.ASSIGN {
			return "", token.NoPos, nil, fmt.Errorf(
				"unsupported predicateEnable initialization in enablePredicate",
			)
		}

		name, ok := assignment.Lhs[0].(*ast.Ident)
		if !ok {
			return "", token.NoPos, nil, fmt.Errorf(
				"unsupported predicateEnable target in enablePredicate",
			)
		}

		if literal != nil {
			return "", token.NoPos, nil, fmt.Errorf(
				"multiple predicateEnable struct literals in enablePredicate",
			)
		}

		predicateName = name.Name
		literal = candidate
	}

	if literal == nil {
		return "", token.NoPos, nil, fmt.Errorf(
			"predicateEnable struct literal is missing from enablePredicate in %s",
			predicatePluginSourcePath,
		)
	}

	defaults := make(map[string]bool)

	for _, element := range literal.Elts {
		keyValue, ok := element.(*ast.KeyValueExpr)
		if !ok {
			return "", token.NoPos, nil, fmt.Errorf(
				"unsupported unkeyed predicateEnable struct literal in enablePredicate",
			)
		}

		field, ok := keyValue.Key.(*ast.Ident)
		if !ok {
			return "", token.NoPos, nil, fmt.Errorf(
				"unsupported predicateEnable field key in enablePredicate",
			)
		}

		if !booleanFields[field.Name] {
			continue
		}

		value, known := booleanLiteral(keyValue.Value)
		if !known {
			return "", token.NoPos, nil, fmt.Errorf(
				"unsupported bool default syntax for predicateEnable.%s",
				field.Name,
			)
		}

		if _, duplicate := defaults[field.Name]; duplicate {
			return "", token.NoPos, nil, fmt.Errorf(
				"predicateEnable.%s default is assigned more than once",
				field.Name,
			)
		}

		defaults[field.Name] = value
	}

	return predicateName, literal.End(), defaults, nil
}

func isPredicateEnableType(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)

	return ok && identifier.Name == "predicateEnable"
}

func predicateStringConstants(file *ast.File) map[string]string {
	constants := make(map[string]string)

	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}

		for _, specification := range general.Specs {
			values, ok := specification.(*ast.ValueSpec)
			if !ok {
				continue
			}

			for index, name := range values.Names {
				if index >= len(values.Values) {
					continue
				}

				literal, ok := values.Values[index].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}

				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					continue
				}

				constants[name.Name] = value
			}
		}
	}

	return constants
}

func predicateGetBoolDefaults(
	function *ast.FuncDecl,
	argumentsName string,
	predicateName string,
	literalEnd token.Pos,
	booleanFields map[string]bool,
	fieldDefaults map[string]bool,
	constants map[string]string,
) (map[string]bool, error) {
	allCalls := make(map[token.Pos]*ast.CallExpr)

	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && isArgumentsGetBoolCall(call, argumentsName) {
			allCalls[call.Pos()] = call
		}

		return true
	})

	recognized := make(map[token.Pos]bool)
	seenFields := make(map[string]bool)
	result := make(map[string]bool)

	for _, statement := range function.Body.List {
		expression, ok := statement.(*ast.ExprStmt)
		if !ok {
			continue
		}

		call, ok := expression.X.(*ast.CallExpr)
		if !ok || !isArgumentsGetBoolCall(call, argumentsName) {
			continue
		}

		if call.Pos() <= literalEnd {
			return nil, fmt.Errorf(
				"unsupported args.GetBool call before predicateEnable initialization",
			)
		}

		fieldName, constantName, err := predicateGetBoolAssociation(
			call,
			predicateName,
			booleanFields,
		)
		if err != nil {
			return nil, err
		}

		defaultValue, found := fieldDefaults[fieldName]
		if !found {
			return nil, fmt.Errorf(
				"predicateEnable.%s has no explicit bool default in enablePredicate",
				fieldName,
			)
		}

		configurationKey, found := constants[constantName]
		if !found || configurationKey == "" {
			return nil, fmt.Errorf(
				"args.GetBool key %s is not a direct non-empty string constant",
				constantName,
			)
		}

		if seenFields[fieldName] {
			return nil, fmt.Errorf(
				"predicateEnable.%s is associated with args.GetBool more than once",
				fieldName,
			)
		}

		if _, duplicate := result[configurationKey]; duplicate {
			return nil, fmt.Errorf(
				"predicate configuration key %q is associated more than once",
				configurationKey,
			)
		}

		seenFields[fieldName] = true
		result[configurationKey] = defaultValue
		recognized[call.Pos()] = true
	}

	for position := range allCalls {
		if !recognized[position] {
			return nil, fmt.Errorf(
				"unsupported args.GetBool syntax in enablePredicate",
			)
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf(
			"enablePredicate in %s contains no reliably parseable args.GetBool defaults",
			predicatePluginSourcePath,
		)
	}

	return result, nil
}

func isArgumentsGetBoolCall(call *ast.CallExpr, argumentsName string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "GetBool" {
		return false
	}

	receiver, ok := selector.X.(*ast.Ident)

	return ok && receiver.Name == argumentsName
}

func predicateGetBoolAssociation(
	call *ast.CallExpr,
	predicateName string,
	booleanFields map[string]bool,
) (string, string, error) {
	if len(call.Args) != 2 {
		return "", "", fmt.Errorf(
			"unsupported args.GetBool syntax: expected two arguments",
		)
	}

	pointer, ok := call.Args[0].(*ast.UnaryExpr)
	if !ok || pointer.Op != token.AND {
		return "", "", fmt.Errorf(
			"unsupported args.GetBool target: expected &%s.<bool field>",
			predicateName,
		)
	}

	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok {
		return "", "", fmt.Errorf(
			"unsupported args.GetBool target: expected &%s.<bool field>",
			predicateName,
		)
	}

	base, ok := selector.X.(*ast.Ident)
	if !ok || base.Name != predicateName || !booleanFields[selector.Sel.Name] {
		return "", "", fmt.Errorf(
			"unsupported args.GetBool target: expected &%s.<bool field>",
			predicateName,
		)
	}

	constant, ok := call.Args[1].(*ast.Ident)
	if !ok {
		return "", "", fmt.Errorf(
			"unsupported args.GetBool key for predicateEnable.%s: expected a constant identifier",
			selector.Sel.Name,
		)
	}

	return selector.Sel.Name, constant.Name, nil
}
