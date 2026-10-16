//! Localization — the Rust twin of the Go `easybus-sdk/i18n` package and the
//! TypeScript `easybus-sdk` i18n core. Typed, extensible-locale localisation
//! for extension-authored strings that reach the model context.
//!
//! Tool DESCRIPTIONS are localized through the manifest (`description` +
//! `descriptions[locale]`). This module covers the OTHER half: the runtime
//! `content` and error text a tool returns, which is fed verbatim to the model.
//!
//! Design (mirrors the Go/TS cores so all SDKs behave identically):
//!
//!   - A Catalog is `{ key: { locale: template } }`; the locale set is an OPEN
//!     map (any BCP-47-ish tag), so adding a language is pure data.
//!   - Templates interpolate `{name}` placeholders.
//!   - Resolution is total: requested locale -> its base language -> the
//!     fallback locale (default `en`) -> its base -> any available -> the key.
//!     A message is never empty, even for an untranslated locale.

use std::collections::HashMap;

/// BASE_LOCALE is the locale every catalog MUST provide (fallback termination).
pub const BASE_LOCALE: &str = "en";

/// A Catalog maps a message key to its per-locale templates.
pub type Catalog = HashMap<String, HashMap<String, String>>;
/// Params holds `{placeholder}` interpolation values.
pub type Params = HashMap<String, String>;

/// base_lang lowercases and strips the region/script subtag: `zh-CN`,
/// `zh_Hans` and `ZH` all collapse to `zh`.
pub fn base_lang(locale: &str) -> String {
    let l = locale.trim().to_lowercase().replace('_', "-");
    match l.find('-') {
        Some(i) => l[..i].to_string(),
        None => l,
    }
}

fn is_ident(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|c| c == b'_' || c.is_ascii_alphanumeric())
}

/// interpolate replaces `{name}` placeholders; an unknown name is left literal.
///
/// Iterates by char (not byte) so multi-byte UTF-8 templates (e.g. Chinese)
/// survive intact.
pub fn interpolate(template: &str, params: Option<&Params>) -> String {
    let Some(params) = params else {
        return template.to_string();
    };
    if params.is_empty() {
        return template.to_string();
    }
    let chars: Vec<char> = template.chars().collect();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '{' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        // Find the closing brace.
        match chars[i + 1..].iter().position(|&c| c == '}') {
            None => {
                out.extend(&chars[i..]);
                break;
            }
            Some(off) => {
                let name: String = chars[i + 1..i + 1 + off].iter().collect();
                if !is_ident(&name) {
                    out.push('{');
                    i += 1;
                    continue;
                }
                match params.get(&name) {
                    Some(v) => out.push_str(v),
                    None => {
                        out.push('{');
                        out.push_str(&name);
                        out.push('}');
                    }
                }
                i += off + 2;
            }
        }
    }
    out
}

/// translate resolves one message. `locale` may be a full tag; matching walks
/// exact -> base(locale) -> fallback -> base(fallback) -> first-available ->
/// the key itself.
pub fn translate(c: &Catalog, key: &str, locale: &str, params: Option<&Params>, fallback: &str) -> String {
    let Some(entry) = c.get(key) else {
        return key.to_string();
    };
    let fb = if fallback.is_empty() { BASE_LOCALE } else { fallback };
    for cand in [locale.to_string(), base_lang(locale), fb.to_string(), base_lang(fb)] {
        if let Some(tpl) = entry.get(&cand) {
            return interpolate(tpl, params);
        }
    }
    // Last resort: any available translation, else the key. Try the common
    // locales first for a deterministic pick.
    for cand in [BASE_LOCALE.to_string(), "zh".to_string()] {
        if let Some(tpl) = entry.get(&cand) {
            return interpolate(tpl, params);
        }
    }
    if let Some(tpl) = entry.values().next() {
        return interpolate(tpl, params);
    }
    key.to_string()
}

/// A Catalog bound to a fallback locale.
pub struct Translator {
    pub catalog: Catalog,
    pub fallback: String,
}

impl Translator {
    /// Bind a catalog with a fallback locale (defaults to BASE_LOCALE).
    pub fn new(catalog: Catalog, fallback: &str) -> Self {
        Translator {
            catalog,
            fallback: if fallback.is_empty() {
                BASE_LOCALE.to_string()
            } else {
                fallback.to_string()
            },
        }
    }

    /// Resolve `key` for `locale`.
    pub fn t(&self, locale: &str, key: &str, params: Option<&Params>) -> String {
        translate(&self.catalog, key, locale, params, &self.fallback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat() -> Catalog {
        let mut c = Catalog::new();
        let mut hello = HashMap::new();
        hello.insert("en".to_string(), "Hello {name}".to_string());
        hello.insert("zh".to_string(), "你好 {name}".to_string());
        hello.insert("ja".to_string(), "こんにちは {name}".to_string());
        c.insert("hello".to_string(), hello);
        let mut plain = HashMap::new();
        plain.insert("en".to_string(), "plain".to_string());
        plain.insert("zh".to_string(), "简单".to_string());
        c.insert("plain".to_string(), plain);
        c
    }

    fn params(name: &str) -> Params {
        let mut p = Params::new();
        p.insert("name".to_string(), name.to_string());
        p
    }

    #[test]
    fn base_lang_cases() {
        for (input, want) in [
            ("zh-CN", "zh"),
            ("zh_Hans", "zh"),
            ("ZH", "zh"),
            ("en", "en"),
            ("fr", "fr"),
            (" zh-TW ", "zh"),
        ] {
            assert_eq!(base_lang(input), want, "{input}");
        }
    }

    #[test]
    fn interpolate_cases() {
        let mut p = Params::new();
        p.insert("a".to_string(), "1".to_string());
        assert_eq!(interpolate("Hi {a} {b}", Some(&p)), "Hi 1 {b}");
        assert_eq!(interpolate("no params", None), "no params");
        assert_eq!(interpolate("{ }", None), "{ }");
    }

    #[test]
    fn translate_cases() {
        let c = cat();
        assert_eq!(translate(&c, "hello", "zh", Some(&params("X")), ""), "你好 X");
        assert_eq!(translate(&c, "hello", "zh-CN", Some(&params("X")), ""), "你好 X");
        assert_eq!(translate(&c, "hello", "ja-JP", Some(&params("X")), ""), "こんにちは X");
        assert_eq!(translate(&c, "hello", "de", Some(&params("X")), ""), "Hello X");
        assert_eq!(translate(&c, "hello", "", Some(&params("X")), ""), "Hello X");
        assert_eq!(translate(&c, "missing", "zh", None, ""), "missing");
    }

    #[test]
    fn translator_cases() {
        let tr = Translator::new(cat(), "");
        assert_eq!(tr.t("zh", "plain", None), "简单");
        assert_eq!(tr.t("en", "plain", None), "plain");
        assert_eq!(tr.t("xx", "plain", None), "plain");
    }
}
