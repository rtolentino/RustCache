//! Line-based text protocol shared by `cache-server` and `web-api`.
//!
//! Requests (one per line, terminated by `\n` or `\r\n`):
//! `PING`, `GET <key>`, `SET <key> <ttl_secs|-> <value...>`, `DEL <key>`,
//! `EXISTS <key>`, `EXPIRE <key> <secs>`, `TTL <key>`, `INCR <key>`.
//!
//! Responses (one line): `OK`, `PONG`, `VALUE <v>`, `NIL`, `INT <n>`, `ERR <msg>`.
//! Keys contain no whitespace; values are the remainder of the line and must not contain CR/LF.

use thiserror::Error;

pub const MAX_KEY_LEN: usize = 256;
pub const MAX_VALUE_LEN: usize = 1024 * 1024;
/// Upper bound for a whole request line.
pub const MAX_LINE_LEN: usize = MAX_VALUE_LEN + MAX_KEY_LEN + 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Get(String),
    Set {
        key: String,
        value: String,
        ttl_secs: Option<u64>,
    },
    Del(String),
    Exists(String),
    Expire {
        key: String,
        secs: u64,
    },
    Ttl(String),
    Incr(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ok,
    Pong,
    Value(String),
    Nil,
    Int(i64),
    Err(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("empty command")]
    Empty,
    #[error("unknown command '{0}'")]
    UnknownCommand(String),
    #[error("wrong number of arguments for '{0}'")]
    Arity(&'static str),
    #[error("invalid key")]
    InvalidKey,
    #[error("value too large or contains line breaks")]
    InvalidValue,
    #[error("invalid integer")]
    InvalidInteger,
    #[error("malformed response")]
    BadResponse,
}

pub fn validate_key(key: &str) -> Result<(), ProtoError> {
    if key.is_empty()
        || key.len() > MAX_KEY_LEN
        || key.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(ProtoError::InvalidKey);
    }
    Ok(())
}

pub fn validate_value(value: &str) -> Result<(), ProtoError> {
    if value.len() > MAX_VALUE_LEN || value.contains(['\r', '\n']) {
        return Err(ProtoError::InvalidValue);
    }
    Ok(())
}

fn split_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.find(char::is_whitespace) {
        Some(i) => (&s[..i], s[i..].trim_start_matches([' ', '\t'])),
        None => (s, ""),
    }
}

fn key_only(rest: &str, name: &'static str) -> Result<String, ProtoError> {
    let (key, extra) = split_word(rest);
    if key.is_empty() || !extra.trim().is_empty() {
        return Err(ProtoError::Arity(name));
    }
    validate_key(key)?;
    Ok(key.to_string())
}

fn parse_u64(s: &str) -> Result<u64, ProtoError> {
    s.parse().map_err(|_| ProtoError::InvalidInteger)
}

impl Command {
    pub fn parse(line: &str) -> Result<Command, ProtoError> {
        let line = line.trim_end_matches(['\r', '\n']);
        let (name, rest) = split_word(line);
        if name.is_empty() {
            return Err(ProtoError::Empty);
        }
        match name.to_ascii_uppercase().as_str() {
            "PING" => Ok(Command::Ping),
            "GET" => Ok(Command::Get(key_only(rest, "GET")?)),
            "DEL" => Ok(Command::Del(key_only(rest, "DEL")?)),
            "EXISTS" => Ok(Command::Exists(key_only(rest, "EXISTS")?)),
            "TTL" => Ok(Command::Ttl(key_only(rest, "TTL")?)),
            "INCR" => Ok(Command::Incr(key_only(rest, "INCR")?)),
            "EXPIRE" => {
                let (key, rest) = split_word(rest);
                let (secs, extra) = split_word(rest);
                if key.is_empty() || secs.is_empty() || !extra.is_empty() {
                    return Err(ProtoError::Arity("EXPIRE"));
                }
                validate_key(key)?;
                Ok(Command::Expire {
                    key: key.to_string(),
                    secs: parse_u64(secs)?,
                })
            }
            "SET" => {
                let (key, rest) = split_word(rest);
                let (ttl, value) = split_word(rest);
                if key.is_empty() || ttl.is_empty() {
                    return Err(ProtoError::Arity("SET"));
                }
                validate_key(key)?;
                validate_value(value)?;
                let ttl_secs = if ttl == "-" {
                    None
                } else {
                    Some(parse_u64(ttl)?)
                };
                Ok(Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    ttl_secs,
                })
            }
            _ => Err(ProtoError::UnknownCommand(name.to_string())),
        }
    }

    /// Encodes the command as a single line without the trailing newline.
    pub fn encode(&self) -> String {
        match self {
            Command::Ping => "PING".into(),
            Command::Get(k) => format!("GET {k}"),
            Command::Del(k) => format!("DEL {k}"),
            Command::Exists(k) => format!("EXISTS {k}"),
            Command::Ttl(k) => format!("TTL {k}"),
            Command::Incr(k) => format!("INCR {k}"),
            Command::Expire { key, secs } => format!("EXPIRE {key} {secs}"),
            Command::Set {
                key,
                value,
                ttl_secs,
            } => match ttl_secs {
                Some(t) => format!("SET {key} {t} {value}"),
                None => format!("SET {key} - {value}"),
            },
        }
    }
}

impl Response {
    pub fn encode(&self) -> String {
        match self {
            Response::Ok => "OK".into(),
            Response::Pong => "PONG".into(),
            Response::Value(v) => format!("VALUE {v}"),
            Response::Nil => "NIL".into(),
            Response::Int(n) => format!("INT {n}"),
            Response::Err(m) => format!("ERR {}", m.replace(['\r', '\n'], " ")),
        }
    }

    pub fn parse(line: &str) -> Result<Response, ProtoError> {
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = match line.split_once(' ') {
            Some((k, r)) => (k, r),
            None => (line, ""),
        };
        match kind {
            "OK" => Ok(Response::Ok),
            "PONG" => Ok(Response::Pong),
            "NIL" => Ok(Response::Nil),
            "VALUE" => Ok(Response::Value(rest.to_string())),
            "INT" => rest
                .parse()
                .map(Response::Int)
                .map_err(|_| ProtoError::InvalidInteger),
            "ERR" => Ok(Response::Err(rest.to_string())),
            _ => Err(ProtoError::BadResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_roundtrip_keeps_spaces_in_value() {
        let c = Command::Set {
            key: "k".into(),
            value: "hello big  world".into(),
            ttl_secs: Some(5),
        };
        assert_eq!(Command::parse(&c.encode()).unwrap(), c);
        let c = Command::Set {
            key: "k".into(),
            value: String::new(),
            ttl_secs: None,
        };
        assert_eq!(Command::parse(&c.encode()).unwrap(), c);
    }

    #[test]
    fn parses_case_insensitive_and_crlf() {
        assert_eq!(
            Command::parse("get a\r\n").unwrap(),
            Command::Get("a".into())
        );
        assert_eq!(Command::parse("PING").unwrap(), Command::Ping);
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(Command::parse("").unwrap_err(), ProtoError::Empty);
        assert!(matches!(
            Command::parse("NOPE x"),
            Err(ProtoError::UnknownCommand(_))
        ));
        assert_eq!(Command::parse("GET").unwrap_err(), ProtoError::Arity("GET"));
        assert_eq!(
            Command::parse("GET a b").unwrap_err(),
            ProtoError::Arity("GET")
        );
        assert_eq!(
            Command::parse("EXPIRE a x").unwrap_err(),
            ProtoError::InvalidInteger
        );
        assert_eq!(
            Command::parse("SET a").unwrap_err(),
            ProtoError::Arity("SET")
        );
        assert_eq!(
            Command::parse("SET a abc v").unwrap_err(),
            ProtoError::InvalidInteger
        );
    }

    #[test]
    fn response_roundtrip() {
        for r in [
            Response::Ok,
            Response::Pong,
            Response::Nil,
            Response::Int(-3),
            Response::Value("a b".into()),
            Response::Err("bad".into()),
        ] {
            assert_eq!(Response::parse(&r.encode()).unwrap(), r);
        }
    }
}
