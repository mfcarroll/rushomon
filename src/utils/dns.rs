// src/utils/dns.rs
use serde::Deserialize;
use worker::*;

#[derive(Deserialize, Debug)]
struct DnsResponse {
    #[serde(rename = "Answer")]
    answer: Option<Vec<DnsAnswer>>,
}

#[derive(Deserialize, Debug)]
struct DnsAnswer {
    data: String,
}

pub async fn verify_dns_txt(domain: &str, expected_token: &str) -> Result<bool> {
    let encoded_domain = urlencoding::encode(domain);
    let url = format!(
        "https://cloudflare-dns.com/dns-query?name={}&type=TXT",
        encoded_domain
    );

    let headers = Headers::new();
    headers.set("Accept", "application/dns-json")?;

    let mut init = RequestInit::new();
    init.with_method(Method::Get).with_headers(headers);

    let req = Request::new_with_init(&url, &init)?;
    let mut response = Fetch::Request(req).send().await?;

    if response.status_code() != 200 {
        console_log!("DNS query failed with status: {}", response.status_code());
        return Ok(false);
    }

    let resp: DnsResponse = response.json().await?;

    if let Some(answers) = resp.answer {
        let expected_content = format!("rushomon-verification={}", expected_token);

        // DNS TXT records should be wrapped in quotes, but check for both
        let expected_quoted = format!("\"{}\"", expected_content);

        return Ok(answers
            .iter()
            .any(|a| a.data.contains(&expected_content) || a.data.contains(&expected_quoted)));
    }

    Ok(false)
}
