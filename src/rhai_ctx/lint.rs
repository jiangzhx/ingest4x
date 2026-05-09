use crate::rhai_ctx::sink_target_constant_name;
use rhai::{ASTNode, Expr, FnCallExpr, Stmt};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

pub(crate) type RhaiLintResult<T> = Result<T, RhaiLintError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RhaiLintError {
    message: String,
}

impl RhaiLintError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for RhaiLintError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for RhaiLintError {}

pub(crate) fn lint_emit_targets(
    module_name: &str,
    source: &str,
    sink_targets: &[String],
) -> RhaiLintResult<()> {
    let constants = sink_target_constants(sink_targets)?;
    let allowed_constants = constants.keys().cloned().collect::<HashSet<_>>();
    let ast = compile_lint_ast(source).map_err(|error| {
        RhaiLintError::new(format!(
            "failed to compile Rhai module `{module_name}`: {error}"
        ))
    })?;
    lint_emit_targets_in_ast(module_name, &ast, &constants, &allowed_constants)
}

fn compile_lint_ast(source: &str) -> Result<rhai::AST, rhai::ParseError> {
    let mut engine = rhai::Engine::new();
    engine.set_max_expr_depths(0, 0);
    engine.compile(source)
}

fn sink_target_constants(sink_targets: &[String]) -> RhaiLintResult<HashMap<String, String>> {
    let mut constants = HashMap::new();
    for sink_target in sink_targets {
        let constant_name = sink_target_constant_name(sink_target);
        if constant_name == "SINK" {
            return Err(RhaiLintError::new(format!(
                "invalid event sink id '{sink_target}' for Rhai constant"
            )));
        }
        if let Some(existing) = constants.insert(constant_name.clone(), sink_target.clone()) {
            return Err(RhaiLintError::new(format!(
                "event sink ids '{existing}' and '{sink_target}' both map to Rhai constant `{constant_name}`"
            )));
        }
    }
    Ok(constants)
}

fn lint_emit_targets_in_ast(
    module_name: &str,
    ast: &rhai::AST,
    constants: &HashMap<String, String>,
    allowed_constants: &HashSet<String>,
) -> RhaiLintResult<()> {
    let mut error = None;
    ast.walk(&mut |path| {
        let Some(node) = path.last() else {
            return true;
        };
        match node {
            ASTNode::Stmt(Stmt::FnCall(call, position))
            | ASTNode::Expr(Expr::FnCall(call, position))
                if call.name == "emit" =>
            {
                error = lint_emit_call(module_name, call, *position, constants, allowed_constants);
                error.is_none()
            }
            _ => true,
        }
    });
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn lint_emit_call(
    module_name: &str,
    call: &FnCallExpr,
    position: rhai::Position,
    constants: &HashMap<String, String>,
    allowed_constants: &HashSet<String>,
) -> Option<RhaiLintError> {
    let Some(target) = call.args.first() else {
        return Some(invalid_emit_target(
            module_name,
            "emit(target, event) requires a sink target constant",
            position,
        ));
    };

    match target {
        Expr::Variable(variable, _, position) => {
            let constant_name = variable.1.as_str();
            if allowed_constants.contains(constant_name) {
                return None;
            }
            Some(invalid_emit_target(
                module_name,
                format!("unknown sink target constant `{constant_name}`").as_str(),
                *position,
            ))
        }
        Expr::StringConstant(sink_target, position) => {
            let constant_name = sink_target_constant_name(sink_target);
            let message = if constants.contains_key(&constant_name) {
                format!("emit target must use sink constant `{constant_name}` instead of string literal")
            } else {
                format!("unknown sink target `{sink_target}`")
            };
            Some(invalid_emit_target(
                module_name,
                message.as_str(),
                *position,
            ))
        }
        other => Some(invalid_emit_target(
            module_name,
            "emit target must be a configured sink constant",
            other.position(),
        )),
    }
}

fn invalid_emit_target(
    module_name: &str,
    message: &str,
    position: rhai::Position,
) -> RhaiLintError {
    RhaiLintError::new(format!(
        "failed to lint Rhai module `{module_name}`: {message} {position}"
    ))
}
