use crate::auth;
use crate::db;
use crate::db::queries::get_code_length_settings;
use crate::kv;
use crate::kv::links::short_code_exists;
use crate::models::{
    Tier,
    link::{Link, LinkStatus},
};
use crate::repositories::tag_repository::validate_and_normalize_tags;
use crate::repositories::{LinkRepository, SettingsRepository, TagRepository};
use crate::utils::{
    generate_short_code_with_length, now_timestamp, short_code::DEFAULT_COLLISION_THRESHOLD,
    validate_short_code, validate_url,
};
use chrono::Datelike;
use worker::d1::D1Database;
use worker::*;

#[derive(Debug, serde::Deserialize)]
struct ImportLinkRow {
    destination_url: String,
    short_code: Option<String>,
    title: Option<String>,
    tags: Option<Vec<String>>,
    expires_at: Option<i64>,
}

#[derive(Debug, serde::Deserialize)]
struct ImportRequest {
    links: Vec<ImportLinkRow>,
}

#[derive(Debug, serde::Serialize)]
struct ImportError {
    row: usize,
    destination_url: String,
    reason: String,
}

#[derive(Debug, serde::Serialize)]
struct ImportWarning {
    row: usize,
    destination_url: String,
    reason: String,
}

#[derive(Debug, serde::Serialize)]
struct ImportResponse {
    created: usize,
    skipped: usize,
    failed: usize,
    errors: Vec<ImportError>,
    warnings: Vec<ImportWarning>,
}

/// Internal helper to generate a unique short code starting at a minimum length
/// and scaling up if collisions are detected.
async fn generate_progressive_short_code(
    kv: &worker::kv::KvStore,
    db: &D1Database,
    env: &Env,
    admin_min_length: usize,
    system_min_length: usize,
) -> Result<String> {
    let collision_threshold = env
        .var("COLLISION_THRESHOLD")
        .ok()
        .and_then(|v| v.to_string().parse::<usize>().ok())
        .unwrap_or(DEFAULT_COLLISION_THRESHOLD);

    let mut current_length = admin_min_length.max(system_min_length);
    let mut total_attempts = 0;
    let mut current_length_attempts = 0;

    loop {
        let code = generate_short_code_with_length(current_length);

        if !short_code_exists(kv, &code).await? {
            return Ok(code);
        }

        total_attempts += 1;
        current_length_attempts += 1;

        // Exhaustion Trigger: Dynamic threshold based on env var
        if current_length_attempts >= collision_threshold {
            current_length += 1;
            current_length_attempts = 0;

            let settings_repo = SettingsRepository::new();
            let _ = settings_repo
                .set_setting(db, "system_min_code_length", &current_length.to_string())
                .await;

            if admin_min_length < current_length {
                let _ = settings_repo
                    .set_setting(db, "min_random_code_length", &current_length.to_string())
                    .await;
            }
        }

        // Scale the ultimate fail-safe based on the threshold too
        if total_attempts > (collision_threshold * 3).max(20) {
            return Err(Error::RustError(
                "Failed to generate unique short code".into(),
            ));
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/links/import",
    tag = "Links",
    summary = "Import links from CSV",
    description = "Bulk-imports links from a CSV payload. Accepts a JSON array of rows parsed from CSV. Each row must have at least a destination_url. Returns counts of created, skipped (duplicate short codes), and failed rows",
    responses(
        (status = 200, description = "Import result with created/skipped/failed counts"),
        (status = 400, description = "Invalid request body"),
        (status = 401, description = "Unauthorized"),
    ),
    security(
        ("Bearer" = []),
        ("session_cookie" = [])
    )
)]
pub async fn handle_import_links(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let user_ctx = match auth::authenticate_request(&req, &ctx).await {
        Ok(ctx) => ctx,
        Err(e) => return Ok(e.into_response()),
    };
    let user_id = &user_ctx.user_id;
    let org_id = &user_ctx.org_id;

    let db = ctx.env.get_binding::<D1Database>("rushomon")?;

    let billing_account = db::get_billing_account_for_org(&db, org_id)
        .await?
        .ok_or_else(|| Error::RustError("No billing account found for organization".to_string()))?;
    let tier = Tier::from_str_value(&billing_account.tier);
    let limits = tier.as_ref().map(|t| t.limits());
    let is_pro_or_above = matches!(
        tier.as_ref(),
        Some(Tier::Pro) | Some(Tier::Business) | Some(Tier::Unlimited)
    );

    let body: ImportRequest = match req.json().await {
        Ok(b) => b,
        Err(_) => return Response::error("Invalid JSON body", 400),
    };

    if body.links.is_empty() {
        return Response::from_json(&ImportResponse {
            created: 0,
            skipped: 0,
            failed: 0,
            errors: vec![],
            warnings: vec![],
        });
    }

    if body.links.len() > 50 {
        return Response::error("Maximum 50 links per import batch", 400);
    }

    let kv = ctx.kv("URL_MAPPINGS")?;
    let now = now_timestamp();
    let year_month = {
        let dt = chrono::Utc::now();
        format!("{}-{:02}", dt.year(), dt.month())
    };

    let lengths = get_code_length_settings(&db).await?;

    let mut created: usize = 0;
    let mut skipped: usize = 0;
    let mut failed: usize = 0;
    let mut errors: Vec<ImportError> = Vec::new();
    let mut warnings: Vec<ImportWarning> = Vec::new();

    let repo = LinkRepository::new();

    for (idx, row) in body.links.iter().enumerate() {
        let row_num = idx + 1;

        let destination_url = match validate_url(&row.destination_url) {
            Ok(url) => url,
            Err(e) => {
                failed += 1;
                errors.push(ImportError {
                    row: row_num,
                    destination_url: row.destination_url.clone(),
                    reason: format!("Invalid URL: {}", e),
                });
                continue;
            }
        };

        if db::is_destination_blacklisted(&db, &destination_url).await? {
            failed += 1;
            errors.push(ImportError {
                row: row_num,
                destination_url: destination_url.clone(),
                reason: "Destination URL is blocked".to_string(),
            });
            continue;
        }

        if let Some(ref tier_limits) = limits
            && let Some(max_links) = tier_limits.max_links_per_month
        {
            let can_create = db::increment_monthly_counter_for_billing_account(
                &db,
                &billing_account.id,
                &year_month,
                max_links,
            )
            .await?;
            if !can_create {
                failed += 1;
                errors.push(ImportError {
                    row: row_num,
                    destination_url: destination_url.clone(),
                    reason: "Monthly link limit reached".to_string(),
                });
                continue;
            }
        }

        let short_code: String;
        if is_pro_or_above && let Some(provided_code) = row.short_code.as_ref() {
            if let Err(e) = validate_short_code(provided_code) {
                skipped += 1;
                errors.push(ImportError {
                    row: row_num,
                    destination_url: destination_url.clone(),
                    reason: format!("Invalid short code: {}", e),
                });
                continue;
            }

            if provided_code.len() < lengths.effective_custom_min {
                skipped += 1;
                errors.push(ImportError {
                    row: row_num,
                    destination_url: destination_url.clone(),
                    reason: format!(
                        "Custom short code must be at least {} characters",
                        lengths.effective_custom_min
                    ),
                });
                continue;
            }

            let mut resolved: Option<String> = None;
            for attempt in 0u32..=10 {
                let candidate = if attempt == 0 {
                    provided_code.clone()
                } else {
                    format!("{}-{}", provided_code, attempt)
                };
                if !short_code_exists(&kv, &candidate).await? {
                    resolved = Some(candidate);
                    break;
                }
            }

            match resolved {
                Some(c) => short_code = c,
                None => {
                    // Fallback to progressive generator if all suffixes fail
                    match generate_progressive_short_code(
                        &kv,
                        &db,
                        &ctx.env,
                        lengths.min_length,
                        lengths.system_min_length,
                    )
                    .await
                    {
                        Ok(c) => {
                            warnings.push(ImportWarning {
                                row: row_num,
                                destination_url: destination_url.clone(),
                                reason: format!(
                                    "Short code '{}' conflicted with an existing link; a random code was assigned",
                                    provided_code
                                ),
                            });
                            short_code = c;
                        }
                        Err(_) => {
                            failed += 1;
                            errors.push(ImportError {
                                row: row_num,
                                destination_url: destination_url.clone(),
                                reason: "Failed to generate a unique short code after conflict"
                                    .to_string(),
                            });
                            continue;
                        }
                    }
                }
            }
        } else {
            match generate_progressive_short_code(
                &kv,
                &db,
                &ctx.env,
                lengths.min_length,
                lengths.system_min_length,
            )
            .await
            {
                Ok(c) => short_code = c,
                Err(_) => {
                    failed += 1;
                    errors.push(ImportError {
                        row: row_num,
                        destination_url: destination_url.clone(),
                        reason: "Failed to generate unique short code".to_string(),
                    });
                    continue;
                }
            }
        }

        let mut normalized_tags = if let Some(ref tags) = row.tags {
            validate_and_normalize_tags(tags).unwrap_or_default()
        } else {
            Vec::new()
        };

        if let Some(ref tier_limits) = limits
            && let Some(max_tags) = tier_limits.max_tags
        {
            let current_tag_count = TagRepository::new()
                .count_distinct_tags_for_billing_account(&db, &billing_account.id)
                .await?;

            let mut new_tag_count = 0;
            if !normalized_tags.is_empty() {
                let existing_tags_query = db.prepare(
                    "SELECT DISTINCT tag_name
                     FROM link_tags lt
                     JOIN organizations o ON lt.org_id = o.id
                     WHERE o.billing_account_id = ?1",
                );
                let existing_tags_result = existing_tags_query
                    .bind(&[billing_account.id.clone().into()])?
                    .all()
                    .await?;
                let existing_tags_set: std::collections::HashSet<String> = existing_tags_result
                    .results::<serde_json::Value>()?
                    .iter()
                    .filter_map(|row| row["tag_name"].as_str().map(|s| s.to_string()))
                    .collect();

                new_tag_count = normalized_tags
                    .iter()
                    .filter(|tag| !existing_tags_set.contains(*tag))
                    .count() as i64;
            }

            if current_tag_count + new_tag_count > max_tags {
                skipped += 1;
                warnings.push(ImportWarning {
                    row: row_num,
                    destination_url: destination_url.clone(),
                    reason: format!(
                        "Tags skipped: would exceed tag limit ({} max). Consider upgrading your plan.",
                        max_tags
                    ),
                });
                normalized_tags.clear();
            }
        }

        let title = row.title.as_ref().and_then(|t| {
            let trimmed = t.trim().to_string();
            if trimmed.is_empty() || trimmed.len() > 200 {
                None
            } else {
                Some(trimmed)
            }
        });

        let link_id = uuid::Uuid::new_v4().to_string();
        let link = Link {
            id: link_id.clone(),
            org_id: org_id.to_string(),
            short_code: short_code.clone(),
            destination_url: destination_url.clone(),
            title,
            created_by: user_id.to_string(),
            created_at: now,
            updated_at: None,
            expires_at: row.expires_at,
            status: LinkStatus::Active,
            click_count: 0,
            tags: normalized_tags.clone(),
            utm_params: None,
            forward_query_params: None,
            redirect_type: "301".to_string(),
        };

        repo.create(&db, &link).await?;

        if !normalized_tags.is_empty() {
            repo.set_tags(&db, &link_id, org_id, &normalized_tags)
                .await?;
        }

        let mapping = link.to_mapping(false);
        kv::store_link_mapping(&kv, org_id, &short_code, &mapping).await?;

        created += 1;
    }

    Response::from_json(&ImportResponse {
        created,
        skipped,
        failed,
        errors,
        warnings,
    })
}
