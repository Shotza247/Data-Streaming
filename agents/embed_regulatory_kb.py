"""
agents/embed_regulatory_kb.py
-------------------------------
Generates ~50 synthetic regulatory document chunks covering South African
tax rules, income thresholds, and filing requirements. Embeds each chunk
with all-MiniLM-L6-v2 and upserts into the Qdrant `regulatory_kb` collection.

Collection: regulatory_kb
Vectors:    384-dim (all-MiniLM-L6-v2), cosine distance
Payload fields per point:
    chunk_id, source_doc, section, text

Usage:
    python agents/embed_regulatory_kb.py

Called by: scripts/seed_qdrant.py
Idempotent: running multiple times upserts the same UUIDs (no duplicates).
"""

import sys
import uuid
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [embed_regulatory_kb] %(message)s",
    datefmt="%H:%M:%S",
)

COLLECTION_NAME = "regulatory_kb"
VECTOR_DIM      = 384

# ── Synthetic Regulatory Knowledge Base ──────────────────────────────────────
# 50 chunks across 5 source documents covering SA tax law topics.
# These are synthetic but representative of real regulatory content structure.

REGULATORY_CHUNKS = [
    # ── SARS Income Tax Act ─────────────────────────────────────────────────
    {"source_doc": "Income_Tax_Act_58_1962", "section": "1",    "text": "Taxable income means the aggregate of the amounts received by or accrued to a taxpayer from a source within or deemed to be within the Republic, less allowable deductions and exemptions under this Act."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "6",    "text": "The primary rebate for the 2024 tax year is R17 235. Taxpayers under 65 are entitled to the primary rebate only."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "6A",   "text": "The secondary rebate of R9 444 is available to taxpayers aged 65 and older. The tertiary rebate of R3 145 applies to taxpayers aged 75 and older."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "10",   "text": "Income exempt from normal tax includes: amounts received as compensation under the Compensation for Occupational Injuries and Diseases Act; government grants for drought relief; bona fide scholarships and bursaries."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "11",   "text": "Deductible expenses include: expenditure actually incurred in the production of income; wear and tear on assets used for trade; bad debts written off; employer contributions to pension and provident funds."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "12H",  "text": "Learnership allowances: an employer may deduct R40 000 per year for each registered learner. An additional R40 000 is claimable upon successful completion of the learnership."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "18A",  "text": "Donations to approved public benefit organisations are deductible up to 10% of taxable income. Excess donations may be carried forward to the next year of assessment."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "20",   "text": "Assessed losses: a taxpayer who sustains an assessed loss in any year of assessment may set off that loss against income in subsequent years, subject to the ring-fencing provisions of section 20A."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "23",   "text": "No deduction shall be made in respect of: domestic or private expenses; expenses incurred in respect of income exempt from tax; penalties and fines of a criminal nature."},
    {"source_doc": "Income_Tax_Act_58_1962", "section": "89bis","text": "Interest on overdue taxes accrues at the rate determined by the Minister from time to time. Currently set at prime plus 2.5% per annum, compounded monthly."},

    # ── Tax Administration Act ───────────────────────────────────────────────
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "25",  "text": "A taxpayer must file a return within the period specified by the Commissioner. Late filing attracts an administrative penalty of R250 per month, up to a maximum of R16 000."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "27",  "text": "An extension for filing may be granted if the taxpayer applies before the due date. The Commissioner may require supporting documentation."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "40",  "text": "SARS may issue an estimated assessment where a taxpayer fails to submit a return or where the return submitted is incorrect. The taxpayer may object to the estimate within 30 business days."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "42",  "text": "Voluntary disclosure: a taxpayer may apply for voluntary disclosure relief before SARS commences an audit. Relief may include remission of understatement penalties and criminal prosecution immunity."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "64",  "text": "SARS may select taxpayers for audit or verification based on risk profiling, random selection, or third-party data matching. The taxpayer must be notified before the commencement of an audit."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "76",  "text": "Understatement penalty rates: substantial understatement 25%; reasonable care not taken 50%; no reasonable grounds 75%; intentional tax evasion 100%; repeat offences doubled."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "102", "text": "Burden of proof: the taxpayer bears the burden of proving that an assessment is incorrect. SARS bears the burden of proving fraud or intentional evasion."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "105", "text": "Objection to assessment must be lodged within 30 business days of the date of the assessment. Late objections may be condoned if reasonable grounds for the delay are shown."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "195", "text": "Criminal offences include: failure to register as a taxpayer; failure to submit a return; providing false or misleading information; obstruction of SARS officials."},
    {"source_doc": "Tax_Administration_Act_28_2011", "section": "234", "text": "SARS may seize and attach assets of a taxpayer who is in default of a tax debt. A warrant issued by the High Court is required for seizure of assets exceeding R100 000."},

    # ── Tax Thresholds & Brackets ────────────────────────────────────────────
    {"source_doc": "SARS_Tax_Tables_2024", "section": "bracket_1", "text": "Tax bracket 1 (2024): Taxable income R0 - R237 100. Tax rate: 18% of each R1."},
    {"source_doc": "SARS_Tax_Tables_2024", "section": "bracket_2", "text": "Tax bracket 2 (2024): Taxable income R237 101 - R370 500. Tax: R42 678 + 26% of amount above R237 100."},
    {"source_doc": "SARS_Tax_Tables_2024", "section": "bracket_3", "text": "Tax bracket 3 (2024): Taxable income R370 501 - R512 800. Tax: R77 362 + 31% of amount above R370 500."},
    {"source_doc": "SARS_Tax_Tables_2024", "section": "bracket_4", "text": "Tax bracket 4 (2024): Taxable income R512 801 - R673 000. Tax: R121 475 + 36% of amount above R512 800."},
    {"source_doc": "SARS_Tax_Tables_2024", "section": "bracket_5", "text": "Tax bracket 5 (2024): Taxable income R673 001 - R857 900. Tax: R179 147 + 39% of amount above R673 000."},
    {"source_doc": "SARS_Tax_Tables_2024", "section": "bracket_6", "text": "Tax bracket 6 (2024): Taxable income R857 901 - R1 817 000. Tax: R251 258 + 41% of amount above R857 900."},
    {"source_doc": "SARS_Tax_Tables_2024", "section": "bracket_7", "text": "Tax bracket 7 (2024): Taxable income above R1 817 000. Tax: R644 489 + 45% of amount above R1 817 000."},
    {"source_doc": "SARS_Tax_Tables_2024", "section": "threshold", "text": "Tax threshold 2024: Individuals under 65: R95 750. Individuals aged 65-74: R148 217. Individuals aged 75+: R165 689. Below threshold means no income tax is payable."},

    # ── VAT Act ──────────────────────────────────────────────────────────────
    {"source_doc": "VAT_Act_89_1991", "section": "7",   "text": "Standard rate of VAT is 15% on taxable supplies of goods and services made in the Republic. Zero-rated supplies attract VAT at 0%."},
    {"source_doc": "VAT_Act_89_1991", "section": "11",  "text": "Zero-rated supplies include: exports of goods; certain foodstuffs (brown bread, dried beans, rice, maize meal, milk, eggs); supply of illuminating paraffin; international transportation services."},
    {"source_doc": "VAT_Act_89_1991", "section": "12",  "text": "Exempt supplies on which no VAT is charged: financial services; residential accommodation; educational services; public road and rail transport; childcare services."},
    {"source_doc": "VAT_Act_89_1991", "section": "23",  "text": "Compulsory VAT registration threshold is R1 million in taxable supplies over any 12-month period. Voluntary registration is permitted at any level of taxable turnover."},
    {"source_doc": "VAT_Act_89_1991", "section": "27",  "text": "VAT returns must be submitted monthly (turnover >R30 million), bi-monthly (default), or annually (approved category D vendors). Payment is due on the last business day of the month following the tax period."},

    # ── Fraud Detection Guidelines ───────────────────────────────────────────
    {"source_doc": "SARS_Fraud_Prevention_Policy", "section": "3.1",  "text": "High-risk indicators for individual taxpayers: sudden increase in income exceeding 300% year-on-year; multiple submissions from the same IP address; addresses that cannot be verified; inconsistent employment history."},
    {"source_doc": "SARS_Fraud_Prevention_Policy", "section": "3.2",  "text": "Phantom employee fraud involves claiming PAYE deductions for non-existent employees. Red flags: payroll size inconsistent with business turnover; employees with no UIF contributions; identical banking details across multiple employees."},
    {"source_doc": "SARS_Fraud_Prevention_Policy", "section": "3.3",  "text": "Refund fraud: fraudulent refund claims are typically identified by: claiming refunds without corresponding income; multiple claims in a short period; bank account changes immediately before refund claim."},
    {"source_doc": "SARS_Fraud_Prevention_Policy", "section": "3.4",  "text": "Identity theft indicators: submissions from unfamiliar IP regions; ID number inconsistencies; date of birth mismatch; multiple tax numbers linked to a single ID; taxpayer denies filing when contacted."},
    {"source_doc": "SARS_Fraud_Prevention_Policy", "section": "4.1",  "text": "SARS uses a risk scoring model to prioritise audit selection. Factors include: compliance history, income volatility, industry sector risk, third-party data discrepancies, and geographic clustering of claims."},
    {"source_doc": "SARS_Fraud_Prevention_Policy", "section": "4.2",  "text": "When fraud is suspected, SARS may: issue a query letter; request supporting documents; conduct a field audit; refer the case to the Criminal Investigation Unit. All contact must be properly documented."},
    {"source_doc": "SARS_Fraud_Prevention_Policy", "section": "5.1",  "text": "Penalties for tax fraud: civil penalty up to 200% of the unpaid tax; criminal prosecution under the Tax Administration Act; imprisonment up to 5 years for deliberate evasion exceeding R500 000."},
    {"source_doc": "SARS_Fraud_Prevention_Policy", "section": "5.2",  "text": "SARS Anonymous Fraud Hotline: 0800 00 2870. Members of the public and employees may report suspected tax fraud confidentially. Whistleblowers are protected under the Protected Disclosures Act."},

    # ── Employer Obligations ─────────────────────────────────────────────────
    {"source_doc": "PAYE_Employer_Guide_2024", "section": "2.1",  "text": "Employers must register for PAYE if they employ individuals earning above the tax threshold. Registration must be completed before the first payroll run."},
    {"source_doc": "PAYE_Employer_Guide_2024", "section": "2.2",  "text": "PAYE must be deducted from employees' remuneration each month and paid over to SARS by the 7th of the following month (or the last business day before the 7th)."},
    {"source_doc": "PAYE_Employer_Guide_2024", "section": "3.1",  "text": "Remuneration includes: salaries, wages, overtime pay, bonuses, leave pay, commissions, pension fund contributions, and any other amount paid to an employee in respect of employment."},
    {"source_doc": "PAYE_Employer_Guide_2024", "section": "3.2",  "text": "Fringe benefits taxable as remuneration include: use of a company vehicle (3.5% of determined value per month or 3.25% if no private maintenance), accommodation, low-interest loans, and group life insurance premiums."},
    {"source_doc": "PAYE_Employer_Guide_2024", "section": "4.1",  "text": "Employer SDL (Skills Development Levy) is 1% of the leviable amount. Employers with annual payroll below R500 000 are exempt from SDL."},
    {"source_doc": "PAYE_Employer_Guide_2024", "section": "5.1",  "text": "UIF (Unemployment Insurance Fund) contributions: 1% from employer, 1% from employee, calculated on remuneration capped at R17 712 per month (R212 544 per year). Contributions are due monthly with PAYE."},
    {"source_doc": "PAYE_Employer_Guide_2024", "section": "6.1",  "text": "IRP5 / IT3(a) certificates must be issued to all employees by 31 May following the tax year-end. Employers submit a reconciliation (EMP501) to SARS by 31 May as well."},
    {"source_doc": "PAYE_Employer_Guide_2024", "section": "7.1",  "text": "Penalties for late PAYE payment: 10% of the outstanding amount for the first month; an additional 10% for each subsequent month of non-payment. Interest is charged at the official rate."},
]


def ensure_collection(client):
    """Create the regulatory_kb collection if it does not exist."""
    from qdrant_client.models import Distance, VectorParams

    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
        )
        log.info(f"Created Qdrant collection: {COLLECTION_NAME}")
    else:
        log.info(f"Collection already exists: {COLLECTION_NAME}")


def run():
    """Embed all regulatory chunks and upsert into Qdrant."""
    from agents.embeddings import embed_batch
    from agents.qdrant_search import get_qdrant_client
    from qdrant_client.models import PointStruct

    client = get_qdrant_client()
    ensure_collection(client)

    texts = [c["text"] for c in REGULATORY_CHUNKS]
    log.info(f"Embedding {len(texts)} regulatory chunks...")
    vectors = embed_batch(texts, batch_size=32, show_progress=True)

    points = []
    for chunk, vector in zip(REGULATORY_CHUNKS, vectors):
        chunk_id = f"{chunk['source_doc']}::{chunk['section']}"
        point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_id))
        points.append(
            PointStruct(
                id=point_id,
                vector=vector,
                payload={
                    "chunk_id":   chunk_id,
                    "source_doc": chunk["source_doc"],
                    "section":    chunk["section"],
                    "text":       chunk["text"],
                },
            )
        )

    client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)
    info = client.get_collection(COLLECTION_NAME)
    log.info(f"Done. Collection '{COLLECTION_NAME}' has {info.points_count} points.")
    return len(points)


if __name__ == "__main__":
    n = run()
    print(f"Upserted {n} regulatory chunks into Qdrant '{COLLECTION_NAME}'.")
