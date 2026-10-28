# Agent Knowledge Base

Markdown documents the fraud and tax agents retrieve from (Qdrant collection `knowledge_base`).

| File | Layer | Authority |
|---|---|---|
| `01_business_context.md` | What our data, streams, signals and model mean | Internal, derived from this repo |
| `02_fraud_detection.md` | Fraud typologies and detection methods | Public frameworks (ACFE, FATF, FIC) |
| `03_regulation_and_compliance.md` | SA tax, FICA, POCA, POPIA | Primary legislation, linked per section |

## Format

Each `## heading` is one retrieval chunk and must contain:

```
Source: <URL or repo path>
Status: internal | reference | unverified | verified (name, date)
```

The agent shows the status in its citations and warns on `unverified`.

## Maintaining it

1. Edit the markdown.
2. Reload: `podman exec streamlit python /scripts/seed_qdrant.py --kb-only` (also re-seeds `regulatory_kb`).
3. Regulation sections stay `unverified` until a person compares them with the linked source and changes the status to `verified (name, date)`.
4. When code changes a rule, signal or threshold, update `01_business_context.md` in the same commit.

The older synthetic `regulatory_kb` collection is only a fallback when `knowledge_base` is empty.
