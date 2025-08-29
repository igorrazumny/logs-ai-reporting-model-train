# SECURITY

- Client-side anonymization mandatory; salts are client-custodied, never shared.
- Accepted inputs: Parquet/JSONL only; no raw emails, IPs, or names.
- Outputs delivered as private container images (GHCR), not via git.
- Logs redact paths, URLs, IPs; reports exclude raw sample rows unless approved.
- Repo access: least privilege, time-boxed invitations.
