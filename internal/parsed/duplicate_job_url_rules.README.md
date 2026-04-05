# Duplicate Job URL Rules

This file documents the schema used by [duplicate_job_url_rules.json](/c:/Users/aaa/Documents/dev/goapplyjob/goapplyjob-golang-backend/internal/parsed/duplicate_job_url_rules.json).

The parsed worker uses these rules to extract a stable duplicate signature from a job URL before falling back to generic normalized-URL matching.

## Goal

Many ATS pages represent the same job with different URLs:

- different subdomains
- tracking params
- `details` vs `apply` pages
- embedded job-board URLs vs canonical job URLs

These rules let us derive a shared identity such as:

- `greenhouse:company/jobs/4676811005`
- `lever:company/3cac0beb-8734-489f-b67c-b37a0ae74ca8`
- `workday:tenant/software-engineer_R12345`

## Root Shape

```json
{
  "version": 1,
  "rules": [
    {
      "name": "greenhouse",
      "description": "Human-readable note",
      "hostSuffixes": ["greenhouse.io"],
      "allowWithoutHost": true,
      "signatures": [
        {
          "name": "company_job_path",
          "parts": [
            { "type": "path_regex", "pattern": "^/([^/]+)/jobs/(\\d+)(?:/.*)?$", "group": 1, "label": "company" },
            { "type": "path_regex", "pattern": "^/([^/]+)/jobs/(\\d+)(?:/.*)?$", "group": 2, "label": "job_id" }
          ]
        }
      ]
    }
  ]
}
```

## Rule Fields

- `name`
  - Stable rule namespace.
  - Becomes part of the duplicate signature key.

- `description`
  - Human-readable explanation only.

- `hostSuffixes`
  - List of hostname suffixes this rule applies to.
  - Matching is suffix-based, so `greenhouse.io` matches:
    - `boards.greenhouse.io`
    - `job-boards.greenhouse.io`

- `allowWithoutHost`
  - Optional.
  - When `true`, the rule may still run even if the URL host does not match `hostSuffixes`.
  - Useful for shared identifiers that may appear on non-canonical domains, such as `gh_jid`.

- `signatures`
  - One rule can have multiple extraction variants.
  - Different variants can still resolve to the same final duplicate identity if they extract the same labeled values.

## Signature Part Types

### `query_param`

Extracts a meaningful query parameter.

```json
{ "type": "query_param", "key": "jobId", "label": "job_id" }
```

### `path_regex`

Extracts one captured group from the path.

```json
{ "type": "path_regex", "pattern": "^/jobs/(\\d+)(?:/.*)?$", "group": 1, "label": "job_id" }
```

Notes:

- `pattern` is matched against the normalized path only.
- `group` defaults to `1` if omitted or invalid.

### `host_label`

Extracts one hostname label by index after:

- lowercasing
- removing `www.`

Example:

- `company.breezy.hr`
- label `0` => `company`

```json
{ "type": "host_label", "key": "0", "label": "tenant" }
```

### `full_host`

Uses the full normalized host.

```json
{ "type": "full_host", "label": "host" }
```

Useful when two different tenants can share the same posting id pattern and host must remain part of the identity.

## Matching Model

Each matching signature produces a key shaped like:

```text
<rule-name>:<label1>=<value1>|<label2>=<value2>|...
```

Important:

- The signature `name` is not part of the final identity.
- This is intentional so `details` and `apply` variants can still match if they extract the same labeled values.

## Guidelines For Adding Rules

- Prefer stable IDs over slugs when available.
- Include tenant/company context when the same ATS can reuse job ids across tenants.
- Use `allowWithoutHost` only when the same stable identifier can safely appear on non-canonical hosts.
- Avoid weak path-only rules that could create false positives across companies.
- If a rule depends on DOM or hidden form fields rather than the URL, do not add it here.
  - This file is for URL-only duplicate detection.

## Good Additions

- `cid + jobId`
- `company + reqId`
- `tenant + posting token`
- `full_host + posting id`

## Bad Additions

- generic `/jobs/:slug` without tenant context
- title-only slugs that can repeat across companies
- DOM/XPath extractors from application forms

## Testing

When adding or changing rules, add a focused test in [service_test.go](/c:/Users/aaa/Documents/dev/goapplyjob/goapplyjob-golang-backend/internal/parsed/service_test.go):

- same identity across two URL variants
- changed signature when the meaningful token changes
- changed signature when tenant/company changes, if tenant is part of the identity
