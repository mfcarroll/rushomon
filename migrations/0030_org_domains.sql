CREATE TABLE org_domains (
    id TEXT PRIMARY KEY,
    org_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    verification_method TEXT NOT NULL DEFAULT 'dns', -- e.g., 'dns', 'oidc', 'manual'
    verification_token TEXT, -- Made nullable! OIDC won't need this.
    is_verified BOOLEAN DEFAULT 0,
    created_at INTEGER NOT NULL,
    verified_at INTEGER,
    FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE
);

-- Multiple orgs can generate challenges, but only one can verify it
CREATE UNIQUE INDEX idx_org_domains_domain ON org_domains(domain) WHERE is_verified = 1;
