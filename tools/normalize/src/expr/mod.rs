//! DIY expression language (vendored from dpe for profile `when` and compute op).
//!
//! Grammar (EBNF):
//!   expression  = or_expr
//!   or_expr     = and_expr , { "||" , and_expr }
//!   and_expr    = not_expr , { "&&" , not_expr }
//!   not_expr    = [ "!" ] , compare
//!   compare     = value [ cmp_op , value ]
//!   cmp_op      = "==" | "!=" | "<=" | ">=" | "<" | ">"
//!   value       = literal | path | call | "(" expression ")"
//!   literal     = number | string | "true" | "false" | "null"
//!   path        = ident , { "." , ident }
//!   call        = ident , "(" , [ value , { "," , value } ] , ")"

pub mod lexer;
pub mod parser;
pub mod eval;

pub use eval::{evaluate, EvalError, Scope};
pub use parser::{parse, Expr, ParseError};
pub use lexer::{tokenize, LexError, Token};

pub fn compile(source: &str) -> Result<Expr, CompileError> {
    let tokens = tokenize(source).map_err(CompileError::Lex)?;
    parse(&tokens).map_err(CompileError::Parse)
}

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("lex error: {0}")]
    Lex(#[from] LexError),
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),
}
