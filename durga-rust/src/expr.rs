//! Minimal boolean expression evaluator for XOR / OR gateway conditions,
//! mirroring the operators supported by the Java recursive-descent evaluator:
//! `==`, `!=`, `>`, `<`, `>=`, `<=`, `&&`, `||`, `!`, parentheses, nested field
//! access, and numeric / string / boolean / null literals.
//!
//! Conditions may be wrapped in `${ ... }` (BPMN style); the wrapper is
//! stripped before evaluation. Field references resolve against the payload via
//! dot-notation.

use serde_json::Value;

use crate::pipeline::field_at;

/// Evaluates a boolean gateway condition against a payload. Returns `false` on
/// any parse or evaluation error (an unsatisfiable branch), matching the
/// conservative behaviour of the generated Java gateways.
pub fn eval_condition(expression: &str, payload: &Value) -> bool {
    let expr = strip_wrapper(expression.trim());
    if expr.is_empty() {
        return false;
    }
    let tokens = match tokenize(expr) {
        Ok(t) => t,
        Err(_) => return false,
    };
    let mut parser = Parser { tokens: &tokens, pos: 0, payload };
    match parser.parse_or() {
        Ok(v) if parser.pos == parser.tokens.len() => truthy(&v),
        _ => false,
    }
}

fn strip_wrapper(expr: &str) -> &str {
    if let Some(inner) = expr.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        inner.trim()
    } else {
        expr
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Num(f64),
    Str(String),
    Ident(String),
    Bool(bool),
    Null,
    Op(String),
    LParen,
    RParen,
}

fn tokenize(s: &str) -> Result<Vec<Token>, ()> {
    let chars: Vec<char> = s.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '\'' | '"' => {
                let quote = c;
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != quote {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(());
                }
                tokens.push(Token::Str(chars[start..i].iter().collect()));
                i += 1;
            }
            '&' | '|' => {
                if i + 1 < chars.len() && chars[i + 1] == c {
                    tokens.push(Token::Op(format!("{c}{c}")));
                    i += 2;
                } else {
                    return Err(());
                }
            }
            '=' | '!' | '<' | '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Op(format!("{c}=")));
                    i += 2;
                } else if c == '!' {
                    tokens.push(Token::Op("!".to_string()));
                    i += 1;
                } else if c == '<' || c == '>' {
                    tokens.push(Token::Op(c.to_string()));
                    i += 1;
                } else {
                    return Err(());
                }
            }
            _ if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) => {
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                tokens.push(Token::Num(text.parse().map_err(|_| ())?));
            }
            _ if is_ident_start(c) => {
                let start = i;
                i += 1;
                while i < chars.len() && is_ident_part(chars[i]) {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                match text.as_str() {
                    "true" => tokens.push(Token::Bool(true)),
                    "false" => tokens.push(Token::Bool(false)),
                    "null" => tokens.push(Token::Null),
                    _ => tokens.push(Token::Ident(text)),
                }
            }
            _ => return Err(()),
        }
    }
    Ok(tokens)
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_part(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '.'
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
    payload: &'a Value,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn parse_or(&mut self) -> Result<Value, ()> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(Token::Op(op)) if op == "||") {
            self.pos += 1;
            let right = self.parse_and()?;
            left = Value::Bool(truthy(&left) || truthy(&right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Value, ()> {
        let mut left = self.parse_not()?;
        while matches!(self.peek(), Some(Token::Op(op)) if op == "&&") {
            self.pos += 1;
            let right = self.parse_not()?;
            left = Value::Bool(truthy(&left) && truthy(&right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Value, ()> {
        if matches!(self.peek(), Some(Token::Op(op)) if op == "!") {
            self.pos += 1;
            let v = self.parse_not()?;
            return Ok(Value::Bool(!truthy(&v)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Value, ()> {
        let left = self.parse_primary()?;
        if let Some(Token::Op(op)) = self.peek() {
            if matches!(op.as_str(), "==" | "!=" | ">" | "<" | ">=" | "<=") {
                let op = op.clone();
                self.pos += 1;
                let right = self.parse_primary()?;
                return Ok(Value::Bool(compare(&left, &op, &right)));
            }
        }
        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Value, ()> {
        match self.peek().cloned() {
            Some(Token::LParen) => {
                self.pos += 1;
                let v = self.parse_or()?;
                if !matches!(self.peek(), Some(Token::RParen)) {
                    return Err(());
                }
                self.pos += 1;
                Ok(v)
            }
            Some(Token::Num(n)) => {
                self.pos += 1;
                Ok(Value::from(n))
            }
            Some(Token::Str(s)) => {
                self.pos += 1;
                Ok(Value::String(s))
            }
            Some(Token::Bool(b)) => {
                self.pos += 1;
                Ok(Value::Bool(b))
            }
            Some(Token::Null) => {
                self.pos += 1;
                Ok(Value::Null)
            }
            Some(Token::Ident(path)) => {
                self.pos += 1;
                Ok(field_at(self.payload, &path).cloned().unwrap_or(Value::Null))
            }
            _ => Err(()),
        }
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

fn compare(left: &Value, op: &str, right: &Value) -> bool {
    if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
        return match op {
            "==" => l == r,
            "!=" => l != r,
            ">" => l > r,
            "<" => l < r,
            ">=" => l >= r,
            "<=" => l <= r,
            _ => false,
        };
    }
    let l = scalar_text(left);
    let r = scalar_text(right);
    match op {
        "==" => l == r,
        "!=" => l != r,
        ">" => l > r,
        "<" => l < r,
        ">=" => l >= r,
        "<=" => l <= r,
        _ => false,
    }
}

fn scalar_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numeric_comparisons() {
        let p = json!({"amount": 936.65});
        assert!(eval_condition("${amount > 500}", &p));
        assert!(!eval_condition("${amount <= 500}", &p));
        assert!(eval_condition("amount >= 936.65", &p));
    }

    #[test]
    fn string_equality_and_logic() {
        let p = json!({"status": "shipped", "vip": true});
        assert!(eval_condition("status == 'shipped'", &p));
        assert!(eval_condition("status == 'shipped' && vip", &p));
        assert!(!eval_condition("status == 'pending' || !vip", &p));
    }

    #[test]
    fn nested_field_and_parens() {
        let p = json!({"customer": {"tier": "gold"}, "amount": 100});
        assert!(eval_condition("(customer.tier == 'gold') && amount < 200", &p));
    }

    #[test]
    fn missing_field_is_falsey() {
        let p = json!({"a": 1});
        assert!(!eval_condition("missing > 0", &p));
        assert!(!eval_condition("garbage $$ syntax", &p));
    }
}
