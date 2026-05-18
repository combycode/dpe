//! Lexer for the expression DSL.

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Literals
    Number(f64),
    String(String),
    True,
    False,
    Null,
    Ident(String),
    // Operators
    Dot,
    Comma,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Bang,
    AmpAmp,
    PipePipe,
    EqEq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug, thiserror::Error)]
pub enum LexError {
    #[error("unterminated string literal at {0}")]
    UnterminatedString(usize),
    #[error("unexpected character '{0}' at {1}")]
    UnexpectedChar(char, usize),
    #[error("invalid number '{0}' at {1}")]
    InvalidNumber(String, usize),
    #[error("stray '|' at {0} (did you mean '||'?)")]
    StrayPipe(usize),
    #[error("stray '&' at {0} (did you mean '&&'?)")]
    StrayAmp(usize),
    #[error("stray '=' at {0} (did you mean '=='?)")]
    StrayEq(usize),
}

pub fn tokenize(src: &str) -> Result<Vec<Token>, LexError> {
    let bytes = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;

        // Whitespace
        if c.is_whitespace() { i += 1; continue; }

        // Single-char punctuation
        match c {
            '(' => { out.push(Token::LParen);   i += 1; continue; }
            ')' => { out.push(Token::RParen);   i += 1; continue; }
            '[' => { out.push(Token::LBracket); i += 1; continue; }
            ']' => { out.push(Token::RBracket); i += 1; continue; }
            ',' => { out.push(Token::Comma);    i += 1; continue; }
            '.' => { out.push(Token::Dot);      i += 1; continue; }
            _ => {}
        }

        // Two-char operators
        if c == '=' && peek(bytes, i+1) == Some('=') {
            out.push(Token::EqEq); i += 2; continue;
        }
        if c == '!' && peek(bytes, i+1) == Some('=') {
            out.push(Token::NotEq); i += 2; continue;
        }
        if c == '<' && peek(bytes, i+1) == Some('=') {
            out.push(Token::LtEq); i += 2; continue;
        }
        if c == '>' && peek(bytes, i+1) == Some('=') {
            out.push(Token::GtEq); i += 2; continue;
        }
        if c == '&' && peek(bytes, i+1) == Some('&') {
            out.push(Token::AmpAmp); i += 2; continue;
        }
        if c == '|' && peek(bytes, i+1) == Some('|') {
            out.push(Token::PipePipe); i += 2; continue;
        }

        match c {
            '<' => { out.push(Token::Lt);   i += 1; continue; }
            '>' => { out.push(Token::Gt);   i += 1; continue; }
            '!' => { out.push(Token::Bang); i += 1; continue; }
            '&' => return Err(LexError::StrayAmp(i)),
            '|' => return Err(LexError::StrayPipe(i)),
            '=' => return Err(LexError::StrayEq(i)),
            _ => {}
        }

        // Strings (single or double quote)
        if c == '"' || c == '\'' {
            let quote = c;
            let start = i;
            i += 1;
            let mut s = String::new();
            let mut terminated = false;
            while i < bytes.len() {
                let b = bytes[i];
                if b == b'\\' && i + 1 < bytes.len() {
                    // Escape sequences are ASCII-only by design (n / t / r /
                    // \ / quote chars). For unknown escapes we preserve the
                    // following byte as a Unicode char — only ASCII follow-
                    // bytes are reachable here because the next branch
                    // below decodes any non-ASCII byte as a UTF-8 char.
                    let nxt = bytes[i+1] as char;
                    s.push(match nxt {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '\\' => '\\',
                        '"' => '"',
                        '\'' => '\'',
                        other => other,
                    });
                    i += 2;
                } else if b == quote as u8 {
                    terminated = true; i += 1; break;
                } else if b < 0x80 {
                    // ASCII fast path.
                    s.push(b as char);
                    i += 1;
                } else {
                    // Multi-byte UTF-8 char: decode the next char from the
                    // source &str (always valid UTF-8). We were on a char
                    // boundary because every previous branch advanced by
                    // exactly one ASCII byte OR by `len_utf8()`. Without
                    // this branch, `bytes[i] as char` would split a
                    // multi-byte sequence into garbage Latin-1 codepoints —
                    // mojibake — and `'файло'` etc. would never match a
                    // real Cyrillic v.sheet at runtime (regression: inbox
                    // 0022).
                    let ch = src[i..].chars().next()
                        .expect("valid UTF-8 char at byte boundary");
                    let len = ch.len_utf8();
                    s.push(ch);
                    i += len;
                }
            }
            if !terminated { return Err(LexError::UnterminatedString(start)); }
            out.push(Token::String(s));
            continue;
        }

        // Numbers (integer + decimal; optional leading -)
        if c.is_ascii_digit() || (c == '-' && peek_digit(bytes, i+1)) {
            let start = i;
            if c == '-' { i += 1; }
            while i < bytes.len() && (bytes[i] as char).is_ascii_digit() { i += 1; }
            if i < bytes.len() && bytes[i] as char == '.' {
                i += 1;
                while i < bytes.len() && (bytes[i] as char).is_ascii_digit() { i += 1; }
            }
            let slice = &src[start..i];
            let n: f64 = slice.parse()
                .map_err(|_| LexError::InvalidNumber(slice.into(), start))?;
            out.push(Token::Number(n));
            continue;
        }

        // Identifiers (including true/false/null keywords)
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < bytes.len() {
                let ch = bytes[i] as char;
                if ch.is_alphanumeric() || ch == '_' { i += 1; } else { break; }
            }
            let ident = &src[start..i];
            out.push(match ident {
                "true"  => Token::True,
                "false" => Token::False,
                "null"  => Token::Null,
                other   => Token::Ident(other.to_string()),
            });
            continue;
        }

        return Err(LexError::UnexpectedChar(c, i));
    }
    Ok(out)
}

fn peek(bytes: &[u8], i: usize) -> Option<char> {
    bytes.get(i).map(|b| *b as char)
}

fn peek_digit(bytes: &[u8], i: usize) -> bool {
    matches!(peek(bytes, i), Some(c) if c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(src: &str) -> Vec<Token> { tokenize(src).unwrap() }

    #[test] fn empty() { assert!(tok("").is_empty()); }
    #[test] fn whitespace_only() { assert!(tok("   \t  \n ").is_empty()); }

    #[test] fn number_integer() { assert_eq!(tok("42"), vec![Token::Number(42.0)]); }
    #[test] fn number_decimal() { assert_eq!(tok("2.5"), vec![Token::Number(2.5)]); }
    #[test] fn number_negative() { assert_eq!(tok("-7"), vec![Token::Number(-7.0)]); }
    #[test] fn number_neg_decimal() { assert_eq!(tok("-0.5"), vec![Token::Number(-0.5)]); }

    #[test] fn string_dbl() { assert_eq!(tok("\"hi\""), vec![Token::String("hi".into())]); }
    #[test] fn string_sgl() { assert_eq!(tok("'hi'"), vec![Token::String("hi".into())]); }
    #[test] fn string_escapes() {
        assert_eq!(tok(r#"'a\nb'"#), vec![Token::String("a\nb".into())]);
        assert_eq!(tok(r#""a\"b""#), vec![Token::String("a\"b".into())]);
    }
    #[test] fn string_unterminated() {
        assert!(matches!(tokenize("'oops"), Err(LexError::UnterminatedString(_))));
    }

    // ─── UTF-8 in string literals (regression: inbox 0022) ──────────
    //
    // Pre-fix the lexer iterated `bytes[i] as char`, splitting every
    // multi-byte UTF-8 sequence into garbage Latin-1 codepoints. So
    // `'файло'` in YAML became mojibake at the Token::String level,
    // and runtime `includes(real_cyrillic_v_sheet, mangled_literal)`
    // was always false. Test pins the bytes match the intended chars.

    #[test] fn string_cyrillic_lowercase_preserved() {
        // 5 chars × 2 bytes/char in UTF-8 → 10 bytes in the source,
        // BUT the Token::String must hold exactly the 5 chars.
        let toks = tok("'файло'");
        let s = match &toks[0] { Token::String(s) => s.clone(), _ => panic!() };
        assert_eq!(s, "файло");
        assert_eq!(s.chars().count(), 5);
        // "файло" = ф(U+0444) а(U+0430) й(U+0439) л(U+043B) о(U+043E)
        assert_eq!(s.as_bytes(), [0xD1,0x84, 0xD0,0xB0, 0xD0,0xB9, 0xD0,0xBB, 0xD0,0xBE]);
    }

    #[test] fn string_with_embedded_cyrillic_substring() {
        // Mixed: ASCII + Cyrillic + ASCII. Round-trip exact.
        let toks = tok("'sheet файлов vendor-x'");
        let s = match &toks[0] { Token::String(s) => s.clone(), _ => panic!() };
        assert_eq!(s, "sheet файлов vendor-x");
    }

    #[test] fn string_emoji_4byte_utf8_preserved() {
        // 4-byte UTF-8 (supplementary plane). Catches the case the
        // 2-byte fix might still get wrong if mis-implemented.
        let toks = tok("'rocket 🚀 go'");
        let s = match &toks[0] { Token::String(s) => s.clone(), _ => panic!() };
        assert_eq!(s, "rocket 🚀 go");
    }

    #[test] fn string_cyrillic_mixed_with_escapes() {
        // Escape sequences must still work alongside Cyrillic content.
        let toks = tok(r#"'файло\n files'"#);
        let s = match &toks[0] { Token::String(s) => s.clone(), _ => panic!() };
        assert_eq!(s, "файло\n files");
    }

    #[test] fn keywords() {
        assert_eq!(tok("true"),  vec![Token::True]);
        assert_eq!(tok("false"), vec![Token::False]);
        assert_eq!(tok("null"),  vec![Token::Null]);
    }

    #[test] fn idents() {
        assert_eq!(tok("env"),    vec![Token::Ident("env".into())]);
        assert_eq!(tok("_under"), vec![Token::Ident("_under".into())]);
        assert_eq!(tok("a1b2"),   vec![Token::Ident("a1b2".into())]);
    }

    #[test] fn operators() {
        assert_eq!(tok("=="), vec![Token::EqEq]);
        assert_eq!(tok("!="), vec![Token::NotEq]);
        assert_eq!(tok("<="), vec![Token::LtEq]);
        assert_eq!(tok(">="), vec![Token::GtEq]);
        assert_eq!(tok("<"),  vec![Token::Lt]);
        assert_eq!(tok(">"),  vec![Token::Gt]);
        assert_eq!(tok("&&"), vec![Token::AmpAmp]);
        assert_eq!(tok("||"), vec![Token::PipePipe]);
        assert_eq!(tok("!"),  vec![Token::Bang]);
    }

    #[test] fn stray_amp_pipe_eq() {
        assert!(matches!(tokenize("a & b"), Err(LexError::StrayAmp(_))));
        assert!(matches!(tokenize("a | b"), Err(LexError::StrayPipe(_))));
        assert!(matches!(tokenize("a = b"), Err(LexError::StrayEq(_))));
    }

    #[test] fn path_with_dots() {
        assert_eq!(tok("env.v.x"), vec![
            Token::Ident("env".into()), Token::Dot,
            Token::Ident("v".into()),   Token::Dot,
            Token::Ident("x".into()),
        ]);
    }

    #[test] fn call_with_args() {
        assert_eq!(tok("normalize(v.name)"), vec![
            Token::Ident("normalize".into()), Token::LParen,
            Token::Ident("v".into()), Token::Dot, Token::Ident("name".into()),
            Token::RParen,
        ]);
    }

    #[test] fn full_expression() {
        let t = tok("v.x >= 5 && !empty(v.y)");
        assert_eq!(t, vec![
            Token::Ident("v".into()), Token::Dot, Token::Ident("x".into()),
            Token::GtEq, Token::Number(5.0), Token::AmpAmp,
            Token::Bang, Token::Ident("empty".into()), Token::LParen,
            Token::Ident("v".into()), Token::Dot, Token::Ident("y".into()),
            Token::RParen,
        ]);
    }

    #[test] fn unexpected_char() {
        assert!(matches!(tokenize("a @ b"), Err(LexError::UnexpectedChar('@', _))));
    }
}
