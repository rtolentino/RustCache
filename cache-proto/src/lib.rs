//! Line-based text protocol shared by `cache-server` and `web-api`.
//!
//! Requests (one per line, terminated by `\n` or `\r\n`):
//! `PING`, `GET <key>`, `SET <key> <ttl_secs|-> <value...>`, `DEL <key>`,
//! `EXISTS <key>`, `EXPIRE <key> <secs>`, `TTL <key>`, `INCR <key>`.
//!
//! Responses (one line): `OK`, `PONG`, `VALUE <v>`, `NIL`, `INT <n>`, `ERR <msg>`.
//! Keys contain no whitespace; values are the remainder of the line and must not contain CR/LF.

use thiserror::Error;

/// Maximum key length in bytes.
pub const MAX_KEY_LEN: usize = 256;
/// Maximum value length in bytes (1 MiB).
pub const MAX_VALUE_LEN: usize = 1024 * 1024;
/// Upper bound for a whole request line.
pub const MAX_LINE_LEN: usize = MAX_VALUE_LEN + MAX_KEY_LEN + 64;

/// A request sent from a client to the cache server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Liveness check; replies `PONG`.
    Ping,
    /// Read the value stored under the key.
    Get(String),
    /// Store a value, replacing any existing one.
    Set {
        /// Key to write.
        key: String,
        /// Value to store (no CR/LF, at most [`MAX_VALUE_LEN`] bytes).
        value: String,
        /// Time to live in seconds; `None` means the key never expires.
        ttl_secs: Option<u64>,
    },
    /// Delete the key.
    Del(String),
    /// Check whether the key exists and has not expired.
    Exists(String),
    /// Set a new time to live on an existing key.
    Expire {
        /// Key to update.
        key: String,
        /// New time to live in seconds from now.
        secs: u64,
    },
    /// Remaining time to live of the key.
    Ttl(String),
    /// Increment the integer value of the key by one.
    Incr(String),
}

/// A reply sent from the cache server to a client (one line on the wire).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// The command succeeded and returns no data (`OK`).
    Ok,
    /// Reply to [`Command::Ping`] (`PONG`).
    Pong,
    /// A stored value (`VALUE <v>`).
    Value(String),
    /// The key does not exist (`NIL`).
    Nil,
    /// An integer result such as a count, TTL or counter value (`INT <n>`).
    Int(i64),
    /// The command failed; carries a human-readable message (`ERR <msg>`).
    Err(String),
}

/// Errors produced while parsing or validating protocol data.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtoError {
    /// The request line was blank.
    #[error("empty command")]
    Empty,
    /// The command name is not supported; carries the name as received.
    #[error("unknown command '{0}'")]
    UnknownCommand(String),
    /// Missing or extra arguments; carries the command name.
    #[error("wrong number of arguments for '{0}'")]
    Arity(&'static str),
    /// Key is empty, too long, or contains whitespace/control characters.
    #[error("invalid key")]
    InvalidKey,
    /// Value is too long or contains CR/LF.
    #[error("value too large or contains line breaks")]
    InvalidValue,
    /// A numeric argument could not be parsed.
    #[error("invalid integer")]
    InvalidInteger,
    /// A reply line did not match any known response form.
    #[error("malformed response")]
    BadResponse,
}

/// Checks a key against the protocol rules.
///
/// # Arguments
/// * `key` - candidate key; must be 1..=[`MAX_KEY_LEN`] bytes with no whitespace or control characters.
///
/// # Errors
/// [`ProtoError::InvalidKey`] if any rule is violated.
pub fn validate_key(key: &str) -> Result<(), ProtoError> {
    if key.is_empty()
        || key.len() > MAX_KEY_LEN
        || key.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(ProtoError::InvalidKey);
    }
    Ok(())
}

/// Checks a value against the protocol rules.
///
/// # Arguments
/// * `value` - candidate value; at most [`MAX_VALUE_LEN`] bytes and no `\r` or `\n`.
///
/// # Errors
/// [`ProtoError::InvalidValue`] if any rule is violated.
pub fn validate_value(value: &str) -> Result<(), ProtoError> {
    if value.len() > MAX_VALUE_LEN || value.contains(['\r', '\n']) {
        return Err(ProtoError::InvalidValue);
    }
    Ok(())
}

/// Splits off the first whitespace-delimited word; returns `(word, rest)` with leading blanks of `rest` removed.
///
/// # Arguments
/// * `s` - input text; leading whitespace is ignored.
fn split_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.find(char::is_whitespace) {
        Some(i) => (&s[..i], s[i..].trim_start_matches([' ', '\t'])),
        None => (s, ""),
    }
}

/// Parses the arguments of a command that takes exactly one key.
///
/// # Arguments
/// * `rest` - text after the command name.
/// * `name` - command name used in the arity error.
///
/// # Errors
/// [`ProtoError::Arity`] for a missing or extra argument, [`ProtoError::InvalidKey`] for a bad key.
fn key_only(rest: &str, name: &'static str) -> Result<String, ProtoError> {
    let (key, extra) = split_word(rest);
    if key.is_empty() || !extra.trim().is_empty() {
        return Err(ProtoError::Arity(name));
    }
    validate_key(key)?;
    Ok(key.to_string())
}

/// Parses an unsigned integer argument.
///
/// # Arguments
/// * `s` - decimal text.
///
/// # Errors
/// [`ProtoError::InvalidInteger`] if `s` is not a valid `u64`.
fn parse_u64(s: &str) -> Result<u64, ProtoError> {
    s.parse().map_err(|_| ProtoError::InvalidInteger)
}

impl Command {
    /// Parses one request line into a command. Command names are case-insensitive.
    ///
    /// # Arguments
    /// * `line` - request line; a trailing `\r\n` or `\n` is ignored.
    ///
    /// # Errors
    /// [`ProtoError`] for blank lines, unknown commands, wrong arity, bad keys/values or bad integers.
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
    /// Encodes the command as a single request line (without the trailing newline).
    ///
    /// The caller must have validated keys and values; this does not re-check them.
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
    /// Encodes the response as a single reply line (without the trailing newline).
    /// CR/LF inside error messages are replaced with spaces.
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

    /// Parses one reply line.
    ///
    /// # Arguments
    /// * `line` - reply line; a trailing `\r\n` or `\n` is ignored.
    ///
    /// # Errors
    /// [`ProtoError::BadResponse`] for an unknown reply kind, [`ProtoError::InvalidInteger`] for a bad `INT` payload.
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
