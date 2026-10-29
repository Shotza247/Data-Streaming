# Business Context: what this platform sees and what its rules mean

Authority: INTERNAL (derived from this repository's code). If this file disagrees with the code, the code wins; update this file.

Every `##` section below is one retrieval chunk. Each carries a `Source:` and a `Status:` line.

## Purpose and scope of the platform

Source: README.md; agents/ package
Status: internal

The platform ingests South African tax-application events in real time (Kafka), cleans and enriches them (Flink SQL), stores them (MinIO, Postgres), and exposes analytics, a fraud model and agents (Streamlit, LangGraph). The agents help an analyst answer two kinds of question:

1. Tax questions: what a rule, rate, threshold, deadline or penalty is.
2. Fraud questions: is this application or customer suspicious, why, and what should happen next.

Agents advise. They do not make final decisions on a taxpayer. Any outcome with legal effect needs human review (see the POPIA section 71 chunk in 03_regulation_and_compliance.md).

## Event streams and their meaning

Source: producers/topic_init.py; producers/*.py
Status: internal

| Kafka topic | Meaning | Key fields |
|---|---|---|
| tax-applications | One tax filing submission | application_id, customer_id, province, taxable_income, employment_type, employment_status, tax_year, filing_status, is_fraud |
| taxpayer-profiles | Reference data about a taxpayer | customer_id, age, risk_score, historical_filings_count, avg_income_3yr, flagged_previously |
| audit-events | Workflow trail for an application | action, actor type (TAXPAYER, SYSTEM, AGENT), customer_id |
| fraud-signals | A rule or detector raised a flag | signal_id, application_id, customer_id, signal_type, severity, detected_at, description |

customer_id is the join key across all streams. Flink job_02 enriches each application with the matching taxpayer profile.

## tax-applications field dictionary

Source: producers/producer_tax_applications.py; flink-jobs/job_05_cleaned.sql
Status: internal

- province: one of Gauteng, Western Cape, KwaZulu-Natal, Eastern Cape, Limpopo, Mpumalanga, North West, Free State, Northern Cape.
- taxable_income: ZAR per year, DOUBLE. Negative values are dropped by the cleaning job.
- employment_type: EMPLOYED, SELF_EMPLOYED, CONTRACTOR, UNEMPLOYED.
- employment_status: FULL_TIME, PART_TIME, SEASONAL, or N/A (only valid for UNEMPLOYED).
- tax_year: 2022/23, 2023/24 or 2024/25.
- filing_status: FIRST_TIME, RETURNING, AMENDED.
- is_fraud: boolean label. See the label caveat chunk below before trusting it.

Expected income ranges per employment_type in the data (ZAR): EMPLOYED 80,000 to 650,000; SELF_EMPLOYED 40,000 to 900,000; CONTRACTOR 60,000 to 750,000; UNEMPLOYED 0 to 30,000. Income far outside the range for its employment_type is an anomaly worth explaining.

## Enriched taxpayer fields

Source: flink-jobs/job_02_enrich.sql
Status: internal

After enrichment each application also carries: age, risk_score (0 to 1, DECIMAL(5,4)), historical_filings_count, avg_income_3yr, flagged_previously. These make comparisons possible: current income versus avg_income_3yr, first-time filers with no history, and customers previously flagged.

## Fraud signal types and what they imply

Source: producers/producer_fraud_signals.py; flink-jobs/job_04_fraud_pattern.sql
Status: internal

| signal_type | Meaning |
|---|---|
| HIGH_FREQUENCY_SUBMISSIONS | Flink: 3 or more applications from one customer inside a 10-minute tumbling window. Severity HIGH. |
| INCOME_SPIKE | Flink: taxable_income above R500,000. Severity MEDIUM. This is a screening threshold, not a legal one. |
| DUPLICATE_APPLICATION | Multiple applications from a customer in a short window. |
| HIGH_INCOME_ANOMALY | Declared income significantly above the historical average. |
| VELOCITY_BREACH | Submission rate above normal behaviour. |
| PROFILE_MISMATCH | Declared employment status conflicts with records held. |
| KNOWN_FRAUD_PATTERN | Matches a known fraudulent filing template. |
| SUSPICIOUS_EMAIL_DOMAIN | Email domain linked to earlier fraudulent accounts. |
| INCOME_INCONSISTENCY | Income differs across a customer's filings. |

Severity scale: LOW, MEDIUM, HIGH, CRITICAL. Unknown severities are normalised to HIGH by job_06 so they are never silently dropped.

## Label caveat: is_fraud is simulated

Source: producers/producer_tax_applications.py
Status: internal; read before using any model metric

In the current data the producer sets is_fraud = True when any of these hold: income was inflated 3 to 8 times (about 4 percent of events), the customer is in a small repeat-filer pool (about 8 percent of events), or a random 1 percent. In addition, for every block of 50 applications the producer injects 2 to 5 highly fraudulent cases at random positions (income 2.5 to 6 times the employment-type ceiling, an unemployed filer with a large income, or a repeat filer with inflated income). The label is therefore a rule-generated stand-in, not a confirmed SARS outcome.

Consequences the agent must state when relevant:
- A model trained on this label learns the generator's rules, not real fraud.
- Model recall and precision describe agreement with the simulator only.
- Real use needs confirmed outcomes (audit results, verified cases) as labels.

## Model scoring bands used by the fraud agent

Source: agents/fraud_agent.py; mlflow/serve_model.py
Status: internal

The `fraud-detector` model returns fraud_probability from province, taxable_income, employment_type, employment_status and filing_status, plus three derived features: income relative to the ceiling for the employment type (income_ratio), log income, and the number of applications the customer has filed so far (customer_filings, a velocity proxy learned from training data). It does not see the fraud signals or enriched taxpayer fields.

The service also returns predicted_fraud, using a decision threshold that was tuned on out-of-fold predictions and stored with the model version. That threshold is the model's own operating point and is usually different from the bands below. The bands below are a separate triage convention for analysts.

- 0.75 and above: HIGH RISK, flag for manual review.
- 0.45 to below 0.75: MEDIUM RISK, monitor and investigate.
- Below 0.45: LOW RISK.

These bands are a starting convention, not calibrated. If the model is unavailable the agent must say so and not guess a probability.

## How the agent should reason about a case

Source: this platform's design
Status: internal

1. Gather evidence: model probability, fired signals, enriched history, similar past cases.
2. Match evidence to a typology in 02_fraud_detection.md and name it.
3. Check the relevant rule or threshold in 03_regulation_and_compliance.md and cite it.
4. State confidence honestly. Say which evidence is missing.
5. Recommend a next step for a human (review, request documents, escalate). Do not state that someone committed fraud. Use "indicators consistent with" language.
6. Never expose personal information (name, email, ID) beyond what the analyst's task needs.
