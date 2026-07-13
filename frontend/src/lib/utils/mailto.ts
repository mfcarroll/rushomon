/**
 * Helpers for building and parsing `mailto:` destination URLs (RFC 6068).
 *
 * The link modal's Email mode collects To/Subject/Body/Cc/Bcc as separate
 * fields and serialises them into a single, correctly percent-encoded
 * `mailto:` string that is submitted as the link's `destination_url`. There is
 * no API change — this is purely a convenience layer over the same field, so
 * mailto links can also be created by pasting a raw string.
 */

export interface MailtoParts {
  to: string;
  subject: string;
  body: string;
  cc: string;
  bcc: string;
}

export function emptyMailtoParts(): MailtoParts {
  return { to: "", subject: "", body: "", cc: "", bcc: "" };
}

/** True if the value looks like a `mailto:` URL. */
export function isMailto(value: string): boolean {
  return value.trim().toLowerCase().startsWith("mailto:");
}

/** Split a comma-separated address list, trim, and drop empties. */
function splitAddresses(list: string): string[] {
  return list
    .split(",")
    .map((a) => a.trim())
    .filter(Boolean);
}

/**
 * Encode a single email address for a mailto URL. `encodeURIComponent` would
 * encode `@` as `%40`, which RFC 6068 allows but mail clients and validators
 * handle inconsistently — keep `@` literal (as every mailto link in the wild
 * does) while still encoding anything else unusual.
 */
function encodeAddress(address: string): string {
  return encodeURIComponent(address).replace(/%40/g, "@");
}

/**
 * Build a canonical `mailto:` string from the composer fields.
 *
 * Uses `encodeURIComponent` throughout so spaces become `%20` (not `+`, which
 * some mail clients render literally in the subject) and newlines in the body
 * become `%0A`.
 */
export function buildMailto(parts: MailtoParts): string {
  const recipients = splitAddresses(parts.to).map(encodeAddress).join(",");

  const query: string[] = [];
  const addAddressList = (key: string, value: string) => {
    const encoded = splitAddresses(value).map(encodeAddress).join(",");
    if (encoded) query.push(`${key}=${encoded}`);
  };
  const addText = (key: string, value: string) => {
    const trimmed = value.trim();
    if (trimmed) query.push(`${key}=${encodeURIComponent(trimmed)}`);
  };

  addAddressList("cc", parts.cc);
  addAddressList("bcc", parts.bcc);
  addText("subject", parts.subject);
  addText("body", parts.body);

  const queryString = query.length ? `?${query.join("&")}` : "";
  return `mailto:${recipients}${queryString}`;
}

function decode(value: string): string {
  try {
    return decodeURIComponent(value.replace(/\+/g, " "));
  } catch {
    return value;
  }
}

/** Normalise a decoded address list to `a@x.com, b@y.com` for display/editing. */
function normalizeList(value: string): string {
  return splitAddresses(value).join(", ");
}

/**
 * Parse a `mailto:` string back into composer fields. Best-effort: unknown
 * params are ignored, and malformed encodings fall back to the raw value.
 */
export function parseMailto(value: string): MailtoParts {
  const parts = emptyMailtoParts();
  if (!isMailto(value)) return parts;

  const rest = value.slice(value.indexOf(":") + 1);
  const qIndex = rest.indexOf("?");
  const addressPart = qIndex >= 0 ? rest.slice(0, qIndex) : rest;
  const queryPart = qIndex >= 0 ? rest.slice(qIndex + 1) : "";

  parts.to = normalizeList(splitAddresses(addressPart).map(decode).join(", "));

  if (queryPart) {
    for (const pair of queryPart.split("&")) {
      if (!pair) continue;
      const eq = pair.indexOf("=");
      const key = (eq >= 0 ? pair.slice(0, eq) : pair).toLowerCase();
      const raw = eq >= 0 ? pair.slice(eq + 1) : "";
      const val = decode(raw);
      switch (key) {
        case "subject":
          parts.subject = val;
          break;
        case "body":
          parts.body = val;
          break;
        case "cc":
          parts.cc = normalizeList(val);
          break;
        case "bcc":
          parts.bcc = normalizeList(val);
          break;
        case "to":
          parts.to = parts.to
            ? `${parts.to}, ${normalizeList(val)}`
            : normalizeList(val);
          break;
      }
    }
  }

  return parts;
}

/** The first recipient address, for compact display (e.g. on a card). */
export function primaryRecipient(mailtoUrl: string): string {
  return parseMailto(mailtoUrl).to.split(",")[0]?.trim() ?? "";
}
