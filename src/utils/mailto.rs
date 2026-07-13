//! Helpers for `mailto:` destination links.
//!
//! `mailto:` destinations are handled differently from http(s) links: a plain
//! `Location: mailto:…` redirect is unreliable across browsers and in-app
//! webviews, so the redirect handler serves a small HTML interstitial that
//! opens the visitor's mail client (see [`render_mailto_interstitial`]). This
//! module owns all mailto-specific parsing, validation, and rendering so the
//! logic stays in one place.

use url::Url;

/// Structural validation of a parsed `mailto:` URL (RFC 6068).
///
/// The recipient is optional — `mailto:?subject=…` is valid and opens a
/// blank-recipient compose window (some workflows want this) — but any address
/// that is provided (in the path or a `to`/`cc`/`bcc` param) must be
/// well-formed. Query params are restricted to an allowlist of `subject`,
/// `body`, `cc`, and `bcc`; this closes off RFC 6068's arbitrary header fields
/// (e.g. `in-reply-to`, `reply-to`), which mail clients mostly ignore but which
/// could otherwise be used to smuggle surprising headers. Only a completely
/// empty `mailto:` (no recipient and no headers) is rejected.
pub fn validate_mailto(url: &Url) -> Result<(), String> {
    // For the mailto scheme, the (comma-separated) address list lives in the path.
    let path = decoded_path(url);
    let path = path.trim();

    // The recipient is optional (`mailto:?subject=…` is valid — it opens a
    // blank-recipient compose window), but any address that IS provided must
    // be well-formed.
    if !path.is_empty() {
        validate_address_list(path)?;
    }

    for (key, value) in url.query_pairs() {
        match key.to_ascii_lowercase().as_str() {
            "subject" | "body" => {}
            "to" | "cc" | "bcc" => {
                validate_address_list(&value)?;
            }
            other => {
                return Err(format!(
                    "Unsupported mailto parameter '{}'. Only subject, body, cc, and bcc are allowed",
                    other
                ));
            }
        }
    }

    // Still reject a bare `mailto:` with nothing at all — a destination with no
    // recipient and no headers is almost certainly a mistake.
    if path.is_empty() && url.query().unwrap_or("").is_empty() {
        return Err("mailto link must include a recipient, subject, or body".to_string());
    }

    Ok(())
}

/// The percent-decoded path of a `mailto:` URL. `Url::path()` returns the raw
/// (still-encoded) form for non-special schemes, so an address like
/// `test%40example.com` (RFC 6068 permits encoding `@`) must be decoded before
/// validation, display, or domain extraction. Query values don't need this —
/// `Url::query_pairs()` decodes them already.
fn decoded_path(url: &Url) -> String {
    let path = url.path();
    urlencoding::decode(path)
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| path.to_string())
}

/// Validate a comma-separated list of email addresses. Empty entries are
/// skipped; the list as a whole must contain at least one address.
fn validate_address_list(list: &str) -> Result<(), String> {
    let mut any = false;
    for addr in list.split(',') {
        let addr = addr.trim();
        if addr.is_empty() {
            continue;
        }
        any = true;
        if !is_plausible_email(addr) {
            return Err(format!("Invalid email address in mailto link: '{}'", addr));
        }
    }
    if !any {
        return Err("mailto link recipient list is empty".to_string());
    }
    Ok(())
}

/// Lightweight structural check for a single email address. Deliberately not a
/// full RFC 5322 parser — just enough to reject obvious garbage: exactly one
/// `@`, non-empty local part, and a dotted domain with no whitespace.
fn is_plausible_email(addr: &str) -> bool {
    if addr.is_empty() || addr.chars().any(char::is_whitespace) {
        return false;
    }
    if addr.matches('@').count() != 1 {
        return false;
    }
    let (local, domain) = match addr.split_once('@') {
        Some(parts) => parts,
        None => return false,
    };
    if local.is_empty() || domain.is_empty() {
        return false;
    }
    // Domain must be dotted with non-empty leading/trailing labels.
    domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.')
}

/// Extract the domain of the first recipient of a `mailto:` URL, lowercased,
/// for domain-based blacklist matching. `Url::host_str()` is `None` for
/// mailto (the address lives in the path), so callers need this instead.
pub fn primary_domain(url: &Url) -> Option<String> {
    first_recipient(url)
        .and_then(|addr| {
            addr.split_once('@')
                .map(|(_, domain)| domain.to_ascii_lowercase())
        })
        .filter(|domain| !domain.is_empty())
}

/// The first recipient address of a `mailto:` URL (path first, then `to=`).
fn first_recipient(url: &Url) -> Option<String> {
    let path = decoded_path(url);
    let path = path.trim();
    if !path.is_empty() {
        return path.split(',').next().map(|s| s.trim().to_string());
    }
    url.query_pairs()
        .find(|(k, _)| k.eq_ignore_ascii_case("to"))
        .and_then(|(_, v)| v.split(',').next().map(|s| s.trim().to_string()))
}

/// Tidy a raw comma-separated address list for display: trim each entry,
/// drop empties, join with ", ".
fn normalize_display_list(raw: &str) -> String {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(", ")
}

/// A human-readable recipient list for display on the interstitial.
fn recipients_display(url: &Url) -> String {
    let path = decoded_path(url);
    let path = path.trim();
    let raw = if !path.is_empty() {
        path.to_string()
    } else {
        get_query_value(url, "to").unwrap_or_default()
    };
    normalize_display_list(&raw)
}

/// The first non-empty value of a query param, matched case-insensitively.
/// `query_pairs()` percent-decodes values, so callers don't need to.
fn get_query_value(url: &Url, key: &str) -> Option<String> {
    url.query_pairs()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Render one `<div class="row">` for the preview card, or an empty string
/// if the value is absent — keeping the card free of blank rows.
fn preview_row(label: &str, value: Option<String>) -> String {
    match value {
        Some(v) => format!(
            "<div class=\"row\"><span class=\"label\">{}</span><span class=\"value\">{}</span></div>\n",
            escape_html(label),
            escape_html(&v)
        ),
        None => String::new(),
    }
}

/// Like [`preview_row`], but preserves line breaks in the value (the message
/// body is the only multi-line field shown).
fn preview_row_multiline(label: &str, value: Option<String>) -> String {
    match value {
        Some(v) => format!(
            "<div class=\"row\"><span class=\"label\">{}</span><span class=\"value value-multiline\">{}</span></div>\n",
            escape_html(label),
            escape_html(&v)
        ),
        None => String::new(),
    }
}

/// The HTML skeleton for the interstitial, kept in a sibling `.html` file for
/// readability (real syntax highlighting, no `format!` brace-doubling). The
/// `<!--HREF-->`, `<!--ROWS-->`, and `<!--JS-->` comment tokens are filled in
/// at render time. Comment tokens are collision-safe: every substituted value
/// is HTML- or JS-escaped and so cannot contain a literal `<`, meaning no
/// substitution can reintroduce a token regardless of replacement order.
const INTERSTITIAL_TEMPLATE: &str = include_str!("mailto_interstitial.html");

/// Render the HTML interstitial that opens the visitor's mail client for a
/// `mailto:` destination. Immediately navigates via JS, with a `meta refresh`
/// fallback for no-JS clients. The page also shows a read-only preview card
/// (To/Cc/Bcc/Subject/Body as plain label:value rows, not input-styled
/// boxes, so it doesn't read as an editable form) plus a single "Open Email
/// App" button — both so the visitor can see where they're being sent
/// before their mail client opens, and so there's an unambiguous action to
/// take if it doesn't open automatically.
///
/// The mailto URL and its parts are embedded in three sink contexts — HTML
/// attributes (`href`, `meta refresh`), HTML text, and a JS string literal —
/// and are escaped independently for each. `url.as_str()` is already
/// percent-encoded by the `url` crate, but we escape again as defense in
/// depth.
pub fn render_mailto_interstitial(url: &Url) -> String {
    let href = escape_html(url.as_str());
    let js = escape_js(url.as_str());

    // Field order mirrors the compose form: To, Cc/Bcc, Subject, Body.
    let mut rows = String::new();
    rows.push_str(&preview_row("To", Some(recipients_display(url))));
    rows.push_str(&preview_row(
        "Cc",
        get_query_value(url, "cc").map(|v| normalize_display_list(&v)),
    ));
    rows.push_str(&preview_row(
        "Bcc",
        get_query_value(url, "bcc").map(|v| normalize_display_list(&v)),
    ));
    rows.push_str(&preview_row("Subject", get_query_value(url, "subject")));
    rows.push_str(&preview_row_multiline("Body", get_query_value(url, "body")));

    INTERSTITIAL_TEMPLATE
        .replace("<!--ROWS-->", &rows)
        .replace("<!--HREF-->", &href)
        .replace("<!--JS-->", &js)
}

/// Escape for HTML text and double-quoted attribute contexts.
fn escape_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            _ => out.push(c),
        }
    }
    out
}

/// Escape for inclusion inside a double-quoted JavaScript string literal.
/// `<`/`>`/`&` are escaped as hex to make a `</script>` breakout impossible.
fn escape_js(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '<' => out.push_str("\\x3C"),
            '>' => out.push_str("\\x3E"),
            '&' => out.push_str("\\x26"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Url {
        Url::parse(s).expect("valid url")
    }

    #[test]
    fn accepts_simple_address() {
        assert!(validate_mailto(&parse("mailto:user@example.com")).is_ok());
    }

    #[test]
    fn accepts_subject_body_cc_bcc() {
        let u = parse(
            "mailto:user@example.com?subject=Hi&body=There&cc=c@example.com&bcc=b@example.com",
        );
        assert!(validate_mailto(&u).is_ok());
    }

    #[test]
    fn accepts_multiple_recipients() {
        assert!(validate_mailto(&parse("mailto:a@example.com,b@example.com")).is_ok());
    }

    #[test]
    fn accepts_percent_encoded_addresses() {
        // RFC 6068 permits percent-encoding `@` in the address; Url::path()
        // returns it still encoded. Regression: the create API 400'd on this.
        let u = parse(
            "mailto:test%40example.com,test2%40example.com?subject=Subject%20test&body=Body%20of%20message",
        );
        assert!(validate_mailto(&u).is_ok());
        assert_eq!(
            primary_domain(&u).as_deref(),
            Some("example.com"),
            "domain extraction must see the decoded address"
        );
        let html = render_mailto_interstitial(&u);
        assert!(
            html.contains("test@example.com"),
            "interstitial must display the decoded address"
        );
    }

    #[test]
    fn accepts_recipient_via_to_param() {
        assert!(validate_mailto(&parse("mailto:?to=a@example.com&subject=Hi")).is_ok());
    }

    #[test]
    fn accepts_no_recipient_with_headers() {
        // The recipient is optional as long as there's some content.
        assert!(validate_mailto(&parse("mailto:?subject=Hi")).is_ok());
        assert!(validate_mailto(&parse("mailto:?body=Just%20a%20body")).is_ok());
        assert!(validate_mailto(&parse("mailto:?cc=c@example.com")).is_ok());
    }

    #[test]
    fn rejects_empty_mailto() {
        // A bare mailto with no recipient and no headers is still rejected.
        assert!(validate_mailto(&parse("mailto:")).is_err());
    }

    #[test]
    fn rejects_bad_address() {
        assert!(validate_mailto(&parse("mailto:not-an-email")).is_err());
        assert!(validate_mailto(&parse("mailto:missing@domain")).is_err());
        assert!(validate_mailto(&parse("mailto:a@b.com,broken")).is_err());
    }

    #[test]
    fn rejects_disallowed_params() {
        // reply-to / arbitrary headers must be rejected (RFC 6068 header smuggling).
        assert!(validate_mailto(&parse("mailto:a@b.com?reply-to=evil@x.com")).is_err());
        assert!(validate_mailto(&parse("mailto:a@b.com?from=spoof@x.com")).is_err());
    }

    #[test]
    fn rejects_bad_cc_bcc() {
        assert!(validate_mailto(&parse("mailto:a@b.com?cc=garbage")).is_err());
    }

    #[test]
    fn primary_domain_from_path_and_to() {
        assert_eq!(
            primary_domain(&parse("mailto:User@Example.COM")).as_deref(),
            Some("example.com")
        );
        assert_eq!(
            primary_domain(&parse("mailto:?to=x@spam.example")).as_deref(),
            Some("spam.example")
        );
    }

    #[test]
    fn interstitial_contains_redirect_and_address() {
        let html = render_mailto_interstitial(&parse("mailto:user@example.com?subject=Hi"));
        // Substring checks stay tolerant of template whitespace (the .html is
        // Prettier-formatted, so exact spacing around tokens can change).
        assert!(html.contains("window.location.href"));
        assert!(html.contains("http-equiv=\"refresh\""));
        assert!(html.contains("user@example.com"));
    }

    #[test]
    fn interstitial_shows_preview_rows() {
        let html = render_mailto_interstitial(&parse(
            "mailto:user@example.com?subject=Hello&body=Line%20one%0ALine%20two&cc=cc@example.com&bcc=bcc@example.com",
        ));
        assert!(html.contains(">To<"));
        assert!(html.contains(">Cc<"));
        assert!(html.contains(">Bcc<"));
        assert!(html.contains(">Subject<"));
        assert!(html.contains(">Body<"));
        assert!(html.contains("Hello"));
        assert!(html.contains("cc@example.com"));
        assert!(html.contains("bcc@example.com"));
        assert!(html.contains("Line one\nLine two"));
        assert!(html.contains("Open Email App"));

        // Field order in the card mirrors the compose form: To, Cc, Bcc, Subject, Body.
        let to_pos = html.find(">To<").unwrap();
        let cc_pos = html.find(">Cc<").unwrap();
        let bcc_pos = html.find(">Bcc<").unwrap();
        let subject_pos = html.find(">Subject<").unwrap();
        let body_pos = html.find(">Body<").unwrap();
        assert!(to_pos < cc_pos);
        assert!(cc_pos < bcc_pos);
        assert!(bcc_pos < subject_pos);
        assert!(subject_pos < body_pos);
    }

    #[test]
    fn interstitial_omits_absent_rows() {
        // No subject/body/cc/bcc supplied: those rows must not render at all.
        let html = render_mailto_interstitial(&parse("mailto:user@example.com"));
        assert!(!html.contains(">Subject<"));
        assert!(!html.contains(">Body<"));
        assert!(!html.contains(">Cc<"));
        assert!(!html.contains(">Bcc<"));
    }

    #[test]
    fn interstitial_escapes_injection_attempts() {
        // A crafted subject that tries to break out of the JS string / HTML.
        let u = parse("mailto:user@example.com?subject=x\"</script><script>alert(1)</script>");
        let html = render_mailto_interstitial(&u);
        // No raw closing script tag other than our own single trailing one.
        assert_eq!(html.matches("</script>").count(), 1);
        // No unescaped injected opening tag.
        assert!(!html.contains("<script>alert(1)"));
    }

    #[test]
    fn escape_js_neutralizes_script_close() {
        let out = escape_js("</script>");
        assert!(!out.contains("</script>"));
        assert!(out.contains("\\x3C"));
    }

    #[test]
    fn escape_html_neutralizes_tags_and_quotes() {
        let out = escape_html("\"><img src=x onerror=alert(1)>");
        assert!(!out.contains('<'));
        assert!(!out.contains('>'));
        assert!(!out.contains('"'));
    }
}
