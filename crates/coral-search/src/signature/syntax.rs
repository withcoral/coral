//! Reads a TypeScript declaration with oxc into the small type tree.
//!
//! This is the only module that knows oxc's syntax tree. It collects the type
//! aliases and interfaces, and picks the callable: the default export, a
//! declared function or `const` that the default export names, or the only
//! function. A construct the line does not show becomes [`Type::Opaque`]. It
//! also keeps each top-level statement's text, so an abridged declaration can
//! show a type as it was declared.

use std::collections::HashMap;

use oxc_allocator::Allocator;
use oxc_ast::ast::{
    BindingPattern, Declaration, ExportDefaultDeclarationKind, Expression, FormalParameters,
    Function, ModuleDeclaration, PropertyKey, Statement, TSInterfaceDeclaration, TSLiteral,
    TSModuleBlock, TSNamespaceDeclaration, TSNamespaceDeclarationBody, TSSignature, TSTupleElement,
    TSType, TSTypeName, TSTypeOperatorOperator, TSTypeParameterDeclaration,
    TSTypeParameterInstantiation,
};
use oxc_parser::Parser;
use oxc_span::{GetSpan, SourceType};

use super::render::Renderer;
use super::tree::{Declarations, DeclaredType, Member, Parameter, Signature, TopLevel, Type};

/// The declarations of `text`, or `None` when it does not parse or declares
/// no callable.
pub(super) fn read(text: &str) -> Option<Declarations> {
    let allocator = Allocator::default();
    let parsed = Parser::new(&allocator, text, SourceType::d_ts()).parse();
    if parsed.fatal_error || !parsed.diagnostics.is_empty() {
        return None;
    }
    let mut scan = Scan {
        text,
        aliases: HashMap::new(),
        functions: Vec::new(),
        default_name: None,
    };
    let mut statements = Vec::with_capacity(parsed.program.body.len());
    let mut previous_end = 0;
    for statement in &parsed.program.body {
        scan.statement(statement);
        let end = statement.span().end as usize;
        statements.push(TopLevel {
            text: text
                .get(previous_end..end)
                .unwrap_or_default()
                .trim()
                .to_string(),
            declares: scan.declared_type(statement),
        });
        previous_end = end;
    }
    let mut declarations = scan.finish()?;
    declarations.statements = statements;
    Some(declarations)
}

/// One function the declaration declares, and whether it is the default
/// export.
struct Candidate {
    name: Option<String>,
    exported_default: bool,
    signature: Signature,
}

struct Scan<'s> {
    text: &'s str,
    aliases: HashMap<String, Type>,
    functions: Vec<Candidate>,
    /// The name in `export default name;`.
    default_name: Option<String>,
}

impl Scan<'_> {
    fn statement(&mut self, statement: &Statement<'_>) {
        if let Some(declaration) = statement.as_declaration() {
            self.declaration(declaration);
            return;
        }
        match statement.as_module_declaration() {
            Some(ModuleDeclaration::ExportDeclaration(export)) => {
                self.declaration(&export.declaration);
            }
            Some(ModuleDeclaration::ExportDefaultDeclaration(export)) => {
                match &export.declaration {
                    ExportDefaultDeclarationKind::FunctionDeclaration(function) => {
                        self.function(function, true);
                    }
                    ExportDefaultDeclarationKind::TSInterfaceDeclaration(interface) => {
                        self.interface(interface);
                    }
                    other => {
                        if let Some(Expression::Identifier(name)) = other.as_expression() {
                            self.default_name = Some(name.name.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn declaration(&mut self, declaration: &Declaration<'_>) {
        match declaration {
            Declaration::TSTypeAliasDeclaration(alias) => {
                let body = self.type_(&alias.type_annotation);
                self.aliases.insert(alias.id.name.to_string(), body);
            }
            Declaration::TSInterfaceDeclaration(interface) => self.interface(interface),
            Declaration::FunctionDeclaration(function) => self.function(function, false),
            Declaration::VariableDeclaration(variables) => {
                for variable in &variables.declarations {
                    if let BindingPattern::BindingIdentifier(name) = &variable.id
                        && let Some(annotation) = &variable.type_annotation
                        && let Type::Function { parameters, result } =
                            self.type_(&annotation.type_annotation)
                    {
                        self.functions.push(Candidate {
                            name: Some(name.name.to_string()),
                            exported_default: false,
                            signature: Signature {
                                parameters,
                                result: Some(*result),
                            },
                        });
                    }
                }
            }
            Declaration::TSNamespaceDeclaration(namespace) => self.namespace(namespace),
            Declaration::TSExternalModuleDeclaration(module) => {
                if let Some(block) = &module.body {
                    self.block(block);
                }
            }
            _ => {}
        }
    }

    /// Declarations inside a namespace read as if at top level.
    fn namespace(&mut self, namespace: &TSNamespaceDeclaration<'_>) {
        match &namespace.body {
            TSNamespaceDeclarationBody::TSModuleBlock(block) => self.block(block),
            TSNamespaceDeclarationBody::TSNamespaceDeclaration(inner) => self.namespace(inner),
        }
    }

    fn block(&mut self, block: &TSModuleBlock<'_>) {
        for statement in &block.body {
            self.statement(statement);
        }
    }

    fn interface(&mut self, interface: &TSInterfaceDeclaration<'_>) {
        let mut parts = interface
            .extends
            .iter()
            .map(|parent| match &parent.type_name {
                TSTypeName::IdentifierReference(name) => Type::Named {
                    name: name.name.to_string(),
                    arguments: self.arguments(parent.type_arguments.as_deref()),
                },
                _ => Type::Opaque,
            })
            .collect::<Vec<_>>();
        let body = Type::Object(self.members(&interface.body.body));
        let type_ = if parts.is_empty() {
            body
        } else {
            parts.push(body);
            Type::Intersection(parts)
        };
        // Interfaces of the same name merge, as TypeScript merges them.
        let name = interface.id.name.to_string();
        let type_ = match self.aliases.remove(&name) {
            Some(earlier) => Type::Intersection(vec![earlier, type_]),
            None => type_,
        };
        self.aliases.insert(name, type_);
    }

    fn function(&mut self, function: &Function<'_>, exported_default: bool) {
        let signature = Signature {
            parameters: self.parameters(&function.params),
            result: function
                .return_type
                .as_ref()
                .map(|annotation| self.type_(&annotation.type_annotation)),
        };
        self.functions.push(Candidate {
            name: function.id.as_ref().map(|id| id.name.to_string()),
            exported_default,
            signature,
        });
    }

    fn finish(mut self) -> Option<Declarations> {
        let chosen = self
            .functions
            .iter()
            .position(|candidate| candidate.exported_default)
            .or_else(|| {
                self.functions.iter().position(|candidate| {
                    candidate.name.is_some() && candidate.name == self.default_name
                })
            })
            .or_else(|| (self.functions.len() == 1).then_some(0))?;
        Some(Declarations {
            aliases: self.aliases,
            callable: self.functions.swap_remove(chosen).signature,
            statements: Vec::new(),
        })
    }

    /// The type alias or interface a top-level statement declares, exported
    /// or not.
    fn declared_type(&self, statement: &Statement<'_>) -> Option<DeclaredType> {
        let declaration = match statement.as_module_declaration() {
            Some(ModuleDeclaration::ExportDeclaration(export)) => Some(&export.declaration),
            Some(ModuleDeclaration::ExportDefaultDeclaration(export)) => {
                return match &export.declaration {
                    ExportDefaultDeclarationKind::TSInterfaceDeclaration(interface) => {
                        Some(DeclaredType {
                            name: interface.id.name.to_string(),
                            parameters: self.parameters_text(interface.type_parameters.as_deref()),
                        })
                    }
                    _ => None,
                };
            }
            _ => statement.as_declaration(),
        }?;
        match declaration {
            Declaration::TSTypeAliasDeclaration(alias) => Some(DeclaredType {
                name: alias.id.name.to_string(),
                parameters: self.parameters_text(alias.type_parameters.as_deref()),
            }),
            Declaration::TSInterfaceDeclaration(interface) => Some(DeclaredType {
                name: interface.id.name.to_string(),
                parameters: self.parameters_text(interface.type_parameters.as_deref()),
            }),
            _ => None,
        }
    }

    fn parameters_text(&self, parameters: Option<&TSTypeParameterDeclaration<'_>>) -> String {
        parameters.map_or_else(String::new, |parameters| {
            self.source(parameters.span).to_string()
        })
    }

    fn parameters(&self, parameters: &FormalParameters<'_>) -> Vec<Parameter> {
        let mut read = parameters
            .items
            .iter()
            .map(|parameter| Parameter {
                name: binding_name(&parameter.pattern),
                optional: parameter.optional,
                rest: false,
                value: parameter
                    .type_annotation
                    .as_ref()
                    .map(|annotation| self.type_(&annotation.type_annotation)),
            })
            .collect::<Vec<_>>();
        if let Some(rest) = &parameters.rest {
            read.push(Parameter {
                name: binding_name(&rest.rest.argument),
                optional: false,
                rest: true,
                value: rest
                    .type_annotation
                    .as_ref()
                    .map(|annotation| self.type_(&annotation.type_annotation)),
            });
        }
        read
    }

    fn type_(&self, type_: &TSType<'_>) -> Type {
        match type_ {
            TSType::TSAnyKeyword(_) => plain("any"),
            TSType::TSBigIntKeyword(_) => plain("bigint"),
            TSType::TSBooleanKeyword(_) => plain("boolean"),
            TSType::TSNeverKeyword(_) => plain("never"),
            TSType::TSNullKeyword(_) => plain("null"),
            TSType::TSNumberKeyword(_) => plain("number"),
            TSType::TSObjectKeyword(_) => plain("object"),
            TSType::TSStringKeyword(_) => plain("string"),
            TSType::TSSymbolKeyword(_) => plain("symbol"),
            TSType::TSUndefinedKeyword(_) => plain("undefined"),
            TSType::TSUnknownKeyword(_) => plain("unknown"),
            TSType::TSVoidKeyword(_) => plain("void"),
            TSType::TSThisType(_) => plain("this"),
            TSType::TSArrayType(array) => Type::Array {
                element: Box::new(self.type_(&array.element_type)),
                readonly: false,
            },
            TSType::TSTypeOperatorType(operator) => match operator.operator {
                TSTypeOperatorOperator::Readonly => match self.type_(&operator.type_annotation) {
                    Type::Array { element, .. } => Type::Array {
                        element,
                        readonly: true,
                    },
                    other => other,
                },
                TSTypeOperatorOperator::Keyof | TSTypeOperatorOperator::Unique => Type::Opaque,
            },
            TSType::TSFunctionType(function) if function.type_parameters.is_none() => {
                Type::Function {
                    parameters: self.parameters(&function.params),
                    result: Box::new(self.type_(&function.return_type.type_annotation)),
                }
            }
            TSType::TSIntersectionType(intersection) => Type::Intersection(
                intersection
                    .types
                    .iter()
                    .map(|part| self.type_(part))
                    .collect(),
            ),
            TSType::TSUnionType(union) => {
                Type::Union(union.types.iter().map(|part| self.type_(part)).collect())
            }
            TSType::TSParenthesizedType(inner) => self.type_(&inner.type_annotation),
            TSType::TSLiteralType(literal) => match &literal.literal {
                // One quote style, so `'a'` and `"a"` read the same.
                TSLiteral::StringLiteral(string) => {
                    serde_json::to_string(string.value.as_str()).map_or(Type::Opaque, Type::Plain)
                }
                TSLiteral::TemplateLiteral(_) => Type::Opaque,
                other => Type::Plain(self.source(other.span()).to_string()),
            },
            TSType::TSTupleType(tuple) => Type::Tuple(
                tuple
                    .element_types
                    .iter()
                    .map(|element| self.tuple_element(element))
                    .collect(),
            ),
            TSType::TSTypeLiteral(literal) => Type::Object(self.members(&literal.members)),
            TSType::TSTypeReference(reference) => Type::Named {
                name: type_name(&reference.type_name),
                arguments: self.arguments(reference.type_arguments.as_deref()),
            },
            _ => Type::Opaque,
        }
    }

    fn tuple_element(&self, element: &TSTupleElement<'_>) -> Type {
        match element {
            TSTupleElement::TSOptionalType(optional) => {
                Type::Optional(Box::new(self.type_(&optional.type_annotation)))
            }
            TSTupleElement::TSRestType(_) => Type::Opaque,
            TSTupleElement::TSNamedTupleMember(member) => {
                let inner = self.tuple_element(&member.element_type);
                if member.optional {
                    Type::Optional(Box::new(inner))
                } else {
                    inner
                }
            }
            other => other
                .as_ts_type()
                .map_or(Type::Opaque, |type_| self.type_(type_)),
        }
    }

    fn arguments(&self, arguments: Option<&TSTypeParameterInstantiation<'_>>) -> Vec<Type> {
        arguments.map_or_else(Vec::new, |arguments| {
            arguments
                .params
                .iter()
                .map(|argument| self.type_(argument))
                .collect()
        })
    }

    fn members(&self, signatures: &[TSSignature<'_>]) -> Vec<Member> {
        signatures
            .iter()
            .map(|signature| match signature {
                TSSignature::TSPropertySignature(property) if !property.computed => {
                    property_name(&property.key).map_or(Member::Opaque, |name| Member::Property {
                        name,
                        optional: property.optional,
                        value: property.type_annotation.as_ref().map_or_else(
                            || plain("any"),
                            |annotation| self.type_(&annotation.type_annotation),
                        ),
                    })
                }
                TSSignature::TSIndexSignature(index) => Member::Index {
                    key: format!(
                        "{}: {}",
                        index.parameter.name,
                        Renderer::default()
                            .plain(&self.type_(&index.parameter.type_annotation.type_annotation))
                    ),
                    value: self.type_(&index.type_annotation.type_annotation),
                },
                _ => Member::Opaque,
            })
            .collect()
    }

    fn source(&self, span: oxc_span::Span) -> &str {
        self.text
            .get(span.start as usize..span.end as usize)
            .unwrap_or_default()
    }
}

fn plain(keyword: &str) -> Type {
    Type::Plain(keyword.to_string())
}

fn type_name(name: &TSTypeName<'_>) -> String {
    match name {
        TSTypeName::IdentifierReference(identifier) => identifier.name.to_string(),
        TSTypeName::QualifiedName(qualified) => {
            format!("{}.{}", type_name(&qualified.left), qualified.right.name)
        }
        TSTypeName::ThisExpression(_) => "this".to_string(),
    }
}

/// A parameter's name; a destructured parameter reads as `input`, since the
/// caller passes one value.
fn binding_name(pattern: &BindingPattern<'_>) -> String {
    match pattern {
        BindingPattern::BindingIdentifier(identifier) => identifier.name.to_string(),
        _ => "input".to_string(),
    }
}

/// A property's name, quoted only when it is not a plain identifier.
fn property_name(key: &PropertyKey<'_>) -> Option<String> {
    match key {
        PropertyKey::StaticIdentifier(identifier) => Some(identifier.name.to_string()),
        other => match other.as_expression()? {
            Expression::StringLiteral(string) => {
                let value = string.value.as_str();
                let plain =
                    value.chars().next().is_some_and(|first| {
                        !first.is_ascii_digit() && is_identifier_character(first)
                    }) && value.chars().all(is_identifier_character);
                if plain {
                    Some(value.to_string())
                } else {
                    serde_json::to_string(value).ok()
                }
            }
            Expression::NumericLiteral(number) => number.raw.map(|raw| raw.to_string()),
            _ => None,
        },
    }
}

fn is_identifier_character(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '$')
}
