//! Recursive-descent parser for the expression DSL. Builds AST.

use super::lexer::Token;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Number(f64),
    String(String),
    Bool(bool),
    Null,
    Array(Vec<Expr>),
    Path(Vec<String>),
    Call(String, Vec<Expr>),
    Not(Box<Expr>),
    BinOp(Op, Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    And, Or,
    Eq, NotEq,
    Lt, LtEq, Gt, GtEq,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("unexpected end of input")]
    UnexpectedEnd,
    #[error("unexpected token: {0:?}")]
    Unexpected(Token),
    #[error("expected {expected}, got {got:?}")]
    Expected { expected: String, got: Option<Token> },
    #[error("trailing tokens: {0:?}")]
    Trailing(Vec<Token>),
    #[error("path cannot start with a digit")]
    BadPath,
}

pub fn parse(tokens: &[Token]) -> Result<Expr, ParseError> {
    if tokens.is_empty() { return Err(ParseError::UnexpectedEnd); }
    let mut p = Parser { tokens, pos: 0 };
    let expr = p.parse_or()?;
    if p.pos < tokens.len() {
        return Err(ParseError::Trailing(tokens[p.pos..].to_vec()));
    }
    Ok(expr)
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Token> { self.tokens.get(self.pos) }
    fn next(&mut self) -> Option<&Token> {
        let t = self.tokens.get(self.pos);
        if t.is_some() { self.pos += 1; }
        t
    }

    fn expect(&mut self, expected: &Token, label: &str) -> Result<(), ParseError> {
        match self.peek() {
            Some(t) if t == expected => { self.pos += 1; Ok(()) }
            other => Err(ParseError::Expected {
                expected: label.into(),
                got: other.cloned(),
            }),
        }
    }

    // ─── or_expr = and_expr ( "||" and_expr )* ─────────────────────────
    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(Token::PipePipe)) {
            self.pos += 1;
            let right = self.parse_and()?;
            left = Expr::BinOp(Op::Or, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // ─── and_expr = not_expr ( "&&" not_expr )* ────────────────────────
    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_not()?;
        while matches!(self.peek(), Some(Token::AmpAmp)) {
            self.pos += 1;
            let right = self.parse_not()?;
            left = Expr::BinOp(Op::And, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // ─── not_expr = "!"? compare ───────────────────────────────────────
    fn parse_not(&mut self) -> Result<Expr, ParseError> {
        if matches!(self.peek(), Some(Token::Bang)) {
            self.pos += 1;
            let inner = self.parse_not()?; // right-associative
            return Ok(Expr::Not(Box::new(inner)));
        }
        self.parse_compare()
    }

    // ─── compare = value (cmp_op value)? ───────────────────────────────
    fn parse_compare(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_value()?;
        let op = match self.peek() {
            Some(Token::EqEq)  => Some(Op::Eq),
            Some(Token::NotEq) => Some(Op::NotEq),
            Some(Token::Lt)    => Some(Op::Lt),
            Some(Token::LtEq)  => Some(Op::LtEq),
            Some(Token::Gt)    => Some(Op::Gt),
            Some(Token::GtEq)  => Some(Op::GtEq),
            _ => None,
        };
        if let Some(op) = op {
            self.pos += 1;
            let right = self.parse_value()?;
            return Ok(Expr::BinOp(op, Box::new(left), Box::new(right)));
        }
        Ok(left)
    }

    // ─── value = literal | path | call | "(" expression ")" ───────────
    fn parse_value(&mut self) -> Result<Expr, ParseError> {
        let tok = self.peek().ok_or(ParseError::UnexpectedEnd)?.clone();
        match tok {
            Token::Number(n) => { self.pos += 1; Ok(Expr::Number(n)) }
            Token::String(s) => { self.pos += 1; Ok(Expr::String(s)) }
            Token::True      => { self.pos += 1; Ok(Expr::Bool(true))  }
            Token::False     => { self.pos += 1; Ok(Expr::Bool(false)) }
            Token::Null      => { self.pos += 1; Ok(Expr::Null) }
            Token::LParen    => {
                self.pos += 1;
                let inner = self.parse_or()?;
                self.expect(&Token::RParen, ")")?;
                Ok(inner)
            }
            Token::LBracket => {
                self.pos += 1;
                let mut items = Vec::new();
                if !matches!(self.peek(), Some(Token::RBracket)) {
                    loop {
                        items.push(self.parse_or()?);
                        if matches!(self.peek(), Some(Token::Comma)) {
                            self.pos += 1;
                        } else { break; }
                    }
                }
                self.expect(&Token::RBracket, "]")?;
                Ok(Expr::Array(items))
            }
            Token::Ident(name) => {
                self.pos += 1;
                // Call form: ident "(" args ")"
                if matches!(self.peek(), Some(Token::LParen)) {
                    self.pos += 1;
                    let mut args = Vec::new();
                    if !matches!(self.peek(), Some(Token::RParen)) {
                        loop {
                            args.push(self.parse_or()?);
                            if matches!(self.peek(), Some(Token::Comma)) {
                                self.pos += 1;
                            } else { break; }
                        }
                    }
                    self.expect(&Token::RParen, ")")?;
                    return Ok(Expr::Call(name, args));
                }
                // Path form: ident ( "." ident )*
                let mut path = vec![name];
                while matches!(self.peek(), Some(Token::Dot)) {
                    self.pos += 1;
                    match self.next() {
                        Some(Token::Ident(s)) => path.push(s.clone()),
                        other => return Err(ParseError::Expected {
                            expected: "identifier after '.'".into(),
                            got: other.cloned(),
                        }),
                    }
                }
                Ok(Expr::Path(path))
            }
            other => Err(ParseError::Unexpected(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::lexer::tokenize;

    fn p(src: &str) -> Expr {
        parse(&tokenize(src).unwrap()).unwrap()
    }

    #[test] fn literal_number() { assert_eq!(p("42"), Expr::Number(42.0)); }
    #[test] fn literal_string() { assert_eq!(p("'hi'"), Expr::String("hi".into())); }
    #[test] fn literal_bool() {
        assert_eq!(p("true"),  Expr::Bool(true));
        assert_eq!(p("false"), Expr::Bool(false));
    }
    #[test] fn literal_null() { assert_eq!(p("null"), Expr::Null); }

    #[test] fn simple_path() {
        assert_eq!(p("env.v.x"), Expr::Path(vec!["env".into(),"v".into(),"x".into()]));
    }

    #[test] fn bare_ident_is_path() {
        assert_eq!(p("v"), Expr::Path(vec!["v".into()]));
    }

    #[test] fn call_no_args() {
        assert_eq!(p("now()"), Expr::Call("now".into(), vec![]));
    }

    #[test] fn call_one_arg() {
        assert_eq!(p("normalize(v.name)"),
            Expr::Call("normalize".into(), vec![Expr::Path(vec!["v".into(),"name".into()])]));
    }

    #[test] fn call_multiple_args() {
        assert_eq!(p("includes(v.s, 'foo')"),
            Expr::Call("includes".into(), vec![
                Expr::Path(vec!["v".into(),"s".into()]),
                Expr::String("foo".into()),
            ]));
    }

    #[test] fn eq_comparison() {
        assert_eq!(p("v.x == 1"),
            Expr::BinOp(Op::Eq,
                Box::new(Expr::Path(vec!["v".into(),"x".into()])),
                Box::new(Expr::Number(1.0))));
    }

    #[test] fn all_comparison_ops() {
        for (src, op) in [
            ("1 == 2", Op::Eq), ("1 != 2", Op::NotEq),
            ("1 < 2",  Op::Lt), ("1 <= 2", Op::LtEq),
            ("1 > 2",  Op::Gt), ("1 >= 2", Op::GtEq),
        ] {
            match p(src) {
                Expr::BinOp(o, _, _) => assert_eq!(o, op, "src: {}", src),
                _ => panic!("not binop: {}", src),
            }
        }
    }

    #[test] fn not_negates() {
        assert_eq!(p("!true"), Expr::Not(Box::new(Expr::Bool(true))));
    }

    #[test] fn double_not() {
        assert_eq!(p("!!true"), Expr::Not(Box::new(Expr::Not(Box::new(Expr::Bool(true))))));
    }

    #[test] fn and_chain_left_assoc() {
        // a && b && c → (a && b) && c
        match p("a && b && c") {
            Expr::BinOp(Op::And, left, right) => {
                assert!(matches!(*right, Expr::Path(_)));
                match *left {
                    Expr::BinOp(Op::And, _, _) => {} ,
                    _ => panic!("expected inner && on left"),
                }
            }
            _ => panic!("expected outer &&"),
        }
    }

    #[test] fn or_chain_left_assoc() {
        match p("a || b || c") {
            Expr::BinOp(Op::Or, left, _) => {
                match *left {
                    Expr::BinOp(Op::Or, _, _) => {},
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
    }

    #[test] fn and_binds_tighter_than_or() {
        // a && b || c && d → (a && b) || (c && d)
        match p("a && b || c && d") {
            Expr::BinOp(Op::Or, left, right) => {
                assert!(matches!(*left,  Expr::BinOp(Op::And, _, _)));
                assert!(matches!(*right, Expr::BinOp(Op::And, _, _)));
            }
            _ => panic!(),
        }
    }

    #[test] fn compare_binds_tighter_than_and() {
        // a == 1 && b == 2 → (a == 1) && (b == 2)
        match p("a == 1 && b == 2") {
            Expr::BinOp(Op::And, left, right) => {
                assert!(matches!(*left,  Expr::BinOp(Op::Eq, _, _)));
                assert!(matches!(*right, Expr::BinOp(Op::Eq, _, _)));
            }
            _ => panic!(),
        }
    }

    #[test] fn not_binds_tighter_than_and() {
        // !a && b → (!a) && b
        match p("!a && b") {
            Expr::BinOp(Op::And, left, _) => {
                assert!(matches!(*left, Expr::Not(_)));
            }
            _ => panic!(),
        }
    }

    #[test] fn parens_change_precedence() {
        // a && (b || c)  vs  a && b || c
        match p("a && (b || c)") {
            Expr::BinOp(Op::And, _, right) => {
                assert!(matches!(*right, Expr::BinOp(Op::Or, _, _)));
            }
            _ => panic!(),
        }
    }

    #[test] fn empty_input_errors() {
        assert!(matches!(parse(&[]), Err(ParseError::UnexpectedEnd)));
    }

    #[test] fn trailing_tokens_error() {
        let t = tokenize("1 2").unwrap();
        assert!(matches!(parse(&t), Err(ParseError::Trailing(_))));
    }

    #[test] fn missing_rparen() {
        // tokenizer accepts this — parser must reject trailing open paren
        let t = tokenize("(a").unwrap();
        assert!(parse(&t).is_err());
    }

    #[test] fn dot_must_be_followed_by_ident() {
        let t = tokenize("v.").unwrap();
        assert!(parse(&t).is_err());
    }

    #[test] fn complex_expression() {
        // v.class.category == 'FOUND' && v.class.confidence > 80
        let e = p("v.class.category == 'FOUND' && v.class.confidence > 80");
        match e {
            Expr::BinOp(Op::And, left, right) => {
                assert!(matches!(*left,  Expr::BinOp(Op::Eq, _, _)));
                assert!(matches!(*right, Expr::BinOp(Op::Gt, _, _)));
            }
            _ => panic!(),
        }
    }
}
