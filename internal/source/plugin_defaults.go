package source

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
)

const (
	schedulerPluginOptionsPath  = "pkg/scheduler/conf/scheduler_conf.go"
	schedulerPluginDefaultsPath = "pkg/scheduler/plugins/defaults.go"
)

// LoadSchedulerPluginDefaults reads the selected Volcano source tree and
// returns the branch-specific default value for each supported PluginOption
// YAML switch. A switch is returned only when its default is explicitly and
// unambiguously assigned by ApplyPluginConfDefaults.
func LoadSchedulerPluginDefaults(worktree string) (map[string]bool, error) {
	root, err := schedulerSourceRoot(worktree)
	if err != nil {
		return nil, err
	}

	optionFields, err := loadSchedulerPluginOptionFields(root)
	if err != nil {
		return nil, err
	}

	return loadSchedulerPluginDefaultValues(root, optionFields)
}

func loadSchedulerPluginOptionFields(root string) (map[string]string, error) {
	candidate, found, err := loadSchedulerSource(root, schedulerPluginOptionsPath)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("Volcano scheduler plugin options source %s is missing", schedulerPluginOptionsPath)
	}

	file, err := parser.ParseFile(
		token.NewFileSet(),
		schedulerPluginOptionsPath,
		candidate.content,
		parser.SkipObjectResolution,
	)
	if err != nil {
		return nil, fmt.Errorf("parse scheduler plugin options %s: %w", schedulerPluginOptionsPath, err)
	}

	var pluginOption *ast.StructType

	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}

		for _, specification := range general.Specs {
			typeSpec, ok := specification.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "PluginOption" {
				continue
			}

			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return nil, fmt.Errorf("PluginOption in %s is not a struct", schedulerPluginOptionsPath)
			}

			if pluginOption != nil {
				return nil, fmt.Errorf("multiple PluginOption declarations in %s", schedulerPluginOptionsPath)
			}

			pluginOption = structure
		}
	}

	if pluginOption == nil {
		return nil, fmt.Errorf("PluginOption declaration is missing from %s", schedulerPluginOptionsPath)
	}

	fields := make(map[string]string)
	yamlFields := make(map[string]string)

	for _, field := range pluginOption.Fields.List {
		if !isBooleanPointer(field.Type) {
			continue
		}

		if len(field.Names) != 1 {
			return nil, fmt.Errorf("PluginOption boolean field in %s must have exactly one name", schedulerPluginOptionsPath)
		}

		fieldName := field.Names[0].Name
		yamlName, err := schedulerPluginOptionYAMLName(field)
		if err != nil {
			return nil, fmt.Errorf("PluginOption.%s: %w", fieldName, err)
		}

		if previous, found := yamlFields[yamlName]; found {
			return nil, fmt.Errorf(
				"PluginOption fields %s and %s share YAML switch %q",
				previous,
				fieldName,
				yamlName,
			)
		}

		fields[fieldName] = yamlName
		yamlFields[yamlName] = fieldName
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("PluginOption in %s has no *bool YAML switches", schedulerPluginOptionsPath)
	}

	return fields, nil
}

func isBooleanPointer(expression ast.Expr) bool {
	pointer, ok := expression.(*ast.StarExpr)
	if !ok {
		return false
	}

	identifier, ok := pointer.X.(*ast.Ident)

	return ok && identifier.Name == "bool"
}

func schedulerPluginOptionYAMLName(field *ast.Field) (string, error) {
	if field.Tag == nil {
		return "", fmt.Errorf("*bool field has no YAML tag")
	}

	tag, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		return "", fmt.Errorf("decode struct tag: %w", err)
	}

	yamlName := strings.TrimSpace(strings.Split(reflect.StructTag(tag).Get("yaml"), ",")[0])
	if yamlName == "" || yamlName == "-" {
		return "", fmt.Errorf("*bool field has no usable YAML tag")
	}

	return yamlName, nil
}

func loadSchedulerPluginDefaultValues(
	root string,
	optionFields map[string]string,
) (map[string]bool, error) {
	candidate, found, err := loadSchedulerSource(root, schedulerPluginDefaultsPath)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("Volcano scheduler plugin defaults source %s is missing", schedulerPluginDefaultsPath)
	}

	file, err := parser.ParseFile(
		token.NewFileSet(),
		schedulerPluginDefaultsPath,
		candidate.content,
		parser.SkipObjectResolution,
	)
	if err != nil {
		return nil, fmt.Errorf("parse scheduler plugin defaults %s: %w", schedulerPluginDefaultsPath, err)
	}

	function, optionName, err := schedulerPluginDefaultsFunction(file)
	if err != nil {
		return nil, err
	}

	booleanVariables := make(map[string]bool)
	defaults := make(map[string]bool)

	for _, statement := range function.Body.List {
		if assignment, ok := statement.(*ast.AssignStmt); ok {
			if err := recordBooleanAssignment(assignment, booleanVariables); err != nil {
				return nil, err
			}

			continue
		}

		conditional, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}

		fieldName, guarded := nilPluginOptionField(conditional.Cond, optionName)
		if !guarded {
			continue
		}

		yamlName, known := optionFields[fieldName]
		if !known {
			return nil, fmt.Errorf(
				"ApplyPluginConfDefaults guards unknown or non-*bool field PluginOption.%s",
				fieldName,
			)
		}

		value, err := pluginOptionDefaultValue(conditional, optionName, fieldName, booleanVariables)
		if err != nil {
			return nil, err
		}

		if _, duplicate := defaults[yamlName]; duplicate {
			return nil, fmt.Errorf("PluginOption.%s default is assigned more than once", fieldName)
		}

		defaults[yamlName] = value
	}

	if len(defaults) == 0 {
		return nil, fmt.Errorf(
			"ApplyPluginConfDefaults in %s contains no reliably parseable PluginOption defaults",
			schedulerPluginDefaultsPath,
		)
	}

	return defaults, nil
}

func schedulerPluginDefaultsFunction(file *ast.File) (*ast.FuncDecl, string, error) {
	var target *ast.FuncDecl

	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "ApplyPluginConfDefaults" {
			continue
		}

		if target != nil {
			return nil, "", fmt.Errorf("multiple ApplyPluginConfDefaults declarations in %s", schedulerPluginDefaultsPath)
		}

		target = function
	}

	if target == nil || target.Body == nil {
		return nil, "", fmt.Errorf("ApplyPluginConfDefaults declaration is missing from %s", schedulerPluginDefaultsPath)
	}

	if target.Type.Params == nil || len(target.Type.Params.List) != 1 {
		return nil, "", fmt.Errorf("ApplyPluginConfDefaults must have one *PluginOption parameter")
	}

	parameter := target.Type.Params.List[0]
	if len(parameter.Names) != 1 || !isPluginOptionPointer(parameter.Type) {
		return nil, "", fmt.Errorf("ApplyPluginConfDefaults must have one named *PluginOption parameter")
	}

	return target, parameter.Names[0].Name, nil
}

func isPluginOptionPointer(expression ast.Expr) bool {
	pointer, ok := expression.(*ast.StarExpr)
	if !ok {
		return false
	}

	switch typed := pointer.X.(type) {
	case *ast.Ident:
		return typed.Name == "PluginOption"
	case *ast.SelectorExpr:
		return typed.Sel.Name == "PluginOption"
	default:
		return false
	}
}

func recordBooleanAssignment(assignment *ast.AssignStmt, values map[string]bool) error {
	if len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
		return nil
	}

	name, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok {
		return nil
	}

	value, known := booleanLiteral(assignment.Rhs[0])
	if !known {
		return nil
	}

	if assignment.Tok != token.DEFINE && assignment.Tok != token.ASSIGN {
		return fmt.Errorf("unsupported boolean assignment to %s in ApplyPluginConfDefaults", name.Name)
	}

	values[name.Name] = value

	return nil
}

func booleanLiteral(expression ast.Expr) (bool, bool) {
	identifier, ok := expression.(*ast.Ident)
	if !ok {
		return false, false
	}

	switch identifier.Name {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func nilPluginOptionField(expression ast.Expr, optionName string) (string, bool) {
	binary, ok := expression.(*ast.BinaryExpr)
	if !ok || binary.Op != token.EQL {
		return "", false
	}

	if isNilIdentifier(binary.Y) {
		return selectedPluginOptionField(binary.X, optionName)
	}

	if isNilIdentifier(binary.X) {
		return selectedPluginOptionField(binary.Y, optionName)
	}

	return "", false
}

func isNilIdentifier(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)

	return ok && identifier.Name == "nil"
}

func selectedPluginOptionField(expression ast.Expr, optionName string) (string, bool) {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}

	base, ok := selector.X.(*ast.Ident)
	if !ok || base.Name != optionName {
		return "", false
	}

	return selector.Sel.Name, true
}

func pluginOptionDefaultValue(
	conditional *ast.IfStmt,
	optionName string,
	fieldName string,
	booleanVariables map[string]bool,
) (bool, error) {
	if conditional.Init != nil || conditional.Else != nil || len(conditional.Body.List) != 1 {
		return false, fmt.Errorf(
			"unsupported default syntax for PluginOption.%s: expected a single assignment in a nil guard",
			fieldName,
		)
	}

	assignment, ok := conditional.Body.List[0].(*ast.AssignStmt)
	if !ok || assignment.Tok != token.ASSIGN || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
		return false, fmt.Errorf(
			"unsupported default syntax for PluginOption.%s: expected a single assignment",
			fieldName,
		)
	}

	assignedField, ok := selectedPluginOptionField(assignment.Lhs[0], optionName)
	if !ok || assignedField != fieldName {
		return false, fmt.Errorf(
			"unsupported default syntax for PluginOption.%s: nil guard and assignment do not match",
			fieldName,
		)
	}

	pointer, ok := assignment.Rhs[0].(*ast.UnaryExpr)
	if !ok || pointer.Op != token.AND {
		return false, fmt.Errorf(
			"unsupported default syntax for PluginOption.%s: expected an addressable boolean variable",
			fieldName,
		)
	}

	variable, ok := pointer.X.(*ast.Ident)
	if !ok {
		return false, fmt.Errorf(
			"unsupported default syntax for PluginOption.%s: expected an addressable boolean variable",
			fieldName,
		)
	}

	value, known := booleanVariables[variable.Name]
	if !known {
		return false, fmt.Errorf(
			"cannot resolve boolean variable %s used as the default for PluginOption.%s",
			variable.Name,
			fieldName,
		)
	}

	return value, nil
}
