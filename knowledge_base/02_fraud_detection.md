# Fraud Detection Knowledge: typologies, indicators and analytic methods

Authority: GENERAL PRACTICE. Typologies follow public frameworks (ACFE, FATF, FIC, SARS public guidance). They are not quotations of SARS internal policy, which is not public.

Status values: `reference` means drawn from a public framework and still needs a human check against the linked source before being treated as authoritative.

## How fraud is detected: the four layers

Source: https://www.acfe.com ; https://www.fatf-gafi.org
Status: reference

1. Rules: explicit tests with a clear reason (income above a threshold, N submissions in a window). Cheap, explainable, easy to audit. Weak against new behaviour.
2. Statistics: outlier and peer-group comparison (income versus others of the same employment_type and province, versus the customer's own history). Finds the unusual, not necessarily the fraudulent.
3. Machine learning: learns patterns from labelled outcomes. Only as good as the labels. Needs monitoring for drift.
4. Network and entity analysis: shared bank accounts, devices, addresses, emails across many taxpayers. Finds organised schemes that single-record checks miss.

Good practice combines all four and keeps a human in the loop for decisions. A single score should never be the only reason for action.

## Typology: income inflation or fabricated income

Source: https://www.acfe.com (financial statement and fraud tree concepts); SARS public fraud awareness material at https://www.sars.gov.za
Status: reference

Indicators: taxable_income far above the range for the employment_type; a sharp jump against avg_income_3yr; round-number or repeated amounts; UNEMPLOYED with large income; income that does not match the employer reconciliation.
Platform signals: INCOME_SPIKE, HIGH_INCOME_ANOMALY, INCOME_INCONSISTENCY.
Why it matters: inflated income can be used to support credit or to create refund claims against fabricated withholding.

## Typology: refund fraud

Source: https://www.sars.gov.za (published warnings about fraudulent refund claims)
Status: reference

Indicators: refund claimed with no matching income; claims soon after a bank-detail change; deductions that are unusually large for the declared income; amended returns filed shortly after a first return; many refunds going to one account.
Platform signals: AMENDED filing_status after a recent filing, audit-events showing bank or contact changes before a refund (REFUND_ISSUED).

## Typology: velocity and duplicate submissions

Source: https://www.fatf-gafi.org (structuring and rapid-activity indicators); platform rule in flink-jobs/job_04_fraud_pattern.sql
Status: reference

Indicators: many applications from one customer in a short window; same customer across several provinces; repeat filers.
Platform signals: HIGH_FREQUENCY_SUBMISSIONS (3 or more in 10 minutes), DUPLICATE_APPLICATION, VELOCITY_BREACH.
Note: legitimate causes exist (an accountant filing, correcting a mistake). Ask for context before concluding.

## Typology: identity theft and account takeover

Source: https://www.fic.gov.za (customer due diligence guidance); https://www.sars.gov.za
Status: reference

Indicators: submission from an unfamiliar region or device; ID number or date-of-birth inconsistencies; several tax references tied to one ID; taxpayer says they did not file; email domain linked to earlier fraud.
Platform signals: SUSPICIOUS_EMAIL_DOMAIN, PROFILE_MISMATCH, KNOWN_FRAUD_PATTERN.

## Typology: false employment or employer fraud

Source: https://www.acfe.com (payroll schemes); https://www.sars.gov.za (employer obligations)
Status: reference

Indicators: employees claimed with no UIF contributions; identical bank details across employees; payroll size inconsistent with business turnover; many CONTRACTOR or SEASONAL claims from one employer.
Platform signals: PROFILE_MISMATCH (employment_status versus records), employment_type and employment_status combinations that do not fit (for example UNEMPLOYED with income above the expected range).

## Typology: money laundering through the tax system

Source: https://www.fic.gov.za ; https://www.fatf-gafi.org
Status: reference

Indicators: income unrelated to known activity; large cash deposits followed by tax payments or refund requests; structuring just below reporting thresholds; use of third parties to file or receive refunds.
This overlaps with FICA reporting duties. See 03_regulation_and_compliance.md.

## Red flags that are weak on their own

Source: platform design; https://www.fatf-gafi.org
Status: reference

Do not escalate on one of these alone: a high income by itself (many people earn it legitimately); a first-time filer; a province; a single late filing; an unusual but explained amendment. Combine at least two independent indicators, or one strong indicator plus a rule breach, before recommending review.

## Evidence quality and explanation standard

Source: platform design; Promotion of Administrative Justice Act (fairness of administrative action)
Status: reference

Every fraud explanation should list: the evidence used, where it came from (model, signal, history, similar case), the typology matched, what is missing, and a recommended human action. Avoid accusing language. Use "indicators consistent with" and give the analyst something checkable.

## Handling a model score

Source: agents/fraud_agent.py; mlflow/train_fraud_model.py
Status: internal

A probability is a ranking aid. A high score with no corroborating signal is a prompt to look, not a finding. A low score does not clear a case that has strong rule signals. When the model is trained on simulated labels, say so (see the label caveat in 01_business_context.md).

## Fraud signal severity guidance

Source: producers/producer_fraud_signals.py
Status: internal

LOW: informational, log only. MEDIUM: review in the normal queue. HIGH: prioritise for review within the working day. CRITICAL: escalate to a senior investigator promptly. Severity is the detector's judgement, so the agent should re-rank using corroborating evidence.
