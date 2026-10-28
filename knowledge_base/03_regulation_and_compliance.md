# Regulation and Compliance Knowledge (South Africa)

Authority: PRIMARY LEGISLATION and regulator guidance, linked per section.

IMPORTANT. Statements here are a working summary prepared without a live check of the source. Every section is `Status: unverified` until a person with tax or legal knowledge has compared it to the linked source and changed the status to `verified (name, date)`. Rates, thresholds and amounts change each year, so the agent must quote the tax year and tell the user to confirm against SARS for anything that affects money or a legal outcome.

Index of authoritative sources:
- SARS primary legislation: https://www.sars.gov.za/legal-counsel/primary-legislation/
- SARS tax rates (individuals): https://www.sars.gov.za/tax-rates/income-tax/rates-of-tax-for-individuals/
- Financial Intelligence Centre: https://www.fic.gov.za
- Information Regulator (POPIA): https://inforegulator.org.za
- South African legislation: https://www.gov.za/documents/acts and https://www.saflii.org

## Tax Administration Act 28 of 2011: purpose and key duties

Source: https://www.sars.gov.za/legal-counsel/primary-legislation/ (Tax Administration Act)
Status: unverified

Governs how taxes are administered: registration, returns, assessments, audits and verification, objections and appeals, penalties and criminal offences. It applies across tax types, which is why fraud and compliance answers usually cite it together with the Income Tax Act.

## Tax Administration Act: returns and late filing

Source: https://www.sars.gov.za/legal-counsel/primary-legislation/ (Tax Administration Act, Chapter 15, administrative non-compliance penalties)
Status: unverified

A taxpayer who must file must do so by the date SARS sets for that tax year. Late filing can attract a fixed-amount administrative penalty per month of non-compliance, with the amount depending on taxable income or assessed loss. Exact amounts are in a table published by SARS and are updated, so quote the current table and do not rely on a remembered figure.

## Tax Administration Act: audit, verification and risk selection

Source: https://www.sars.gov.za/legal-counsel/primary-legislation/ (Tax Administration Act, Chapter 5)
Status: unverified

SARS may select returns for verification or audit using risk assessment, third-party data matching, or random selection, and may request relevant material. Analytics-based risk scoring in this platform follows the same logic: it prioritises human review and is not an assessment.

## Tax Administration Act: understatement penalties

Source: https://www.sars.gov.za/legal-counsel/primary-legislation/ (Tax Administration Act, Chapter 16)
Status: unverified

An understatement penalty is a percentage of the shortfall, applied by behaviour class: substantial understatement, failure to take reasonable care, no reasonable grounds for a tax position, gross negligence, and intentional evasion, with the percentage rising for more serious conduct. Percentages and the repeat-offence rule must be taken from the Act and not from memory. Note: the earlier synthetic regulatory_kb collection uses different section numbers and percentages for this topic and should not be trusted.

## Tax Administration Act: criminal offences, evasion and fraud

Source: https://www.sars.gov.za/legal-counsel/primary-legislation/ (Tax Administration Act, Chapter 17)
Status: unverified

Chapter 17 lists tax offences, including failing to file, giving false or misleading information, and evasion of tax, with imprisonment and fines on conviction. Prosecution is decided by the state, and fraud must be proved, not presumed. The agent never states a person has committed an offence. It can say that indicators match a pattern that may fall under the offence provisions and recommend human review.

## Tax Administration Act: objections and disputes

Source: https://www.sars.gov.za/legal-counsel/primary-legislation/ (Tax Administration Act, Chapter 9)
Status: unverified

A taxpayer can object to an assessment within a statutory period and appeal further. This matters for fairness: a flagged taxpayer must be able to respond, and any action taken from agent output must leave that route open.

## Income Tax Act 58 of 1962: taxable income and rates

Source: https://www.sars.gov.za/tax-rates/income-tax/rates-of-tax-for-individuals/
Status: unverified

Individuals are taxed on a progressive scale after rebates. The tax-year tables (brackets, primary, secondary and tertiary rebates, tax thresholds by age) change yearly. The agent must ask which tax year is meant, because the producers use 2022/23, 2023/24 and 2024/25. The older synthetic regulatory_kb has 2024 figures stored as plain text and has no year check.

## Income Tax Act: deductions, exemptions and what is not deductible

Source: https://www.sars.gov.za/legal-counsel/primary-legislation/ (Income Tax Act, sections 10, 11, 18A and 23)
Status: unverified

Deductions generally need expenditure incurred in producing income and not of a private or domestic nature. Retirement contributions and approved public benefit donations have separate limits. Large deductions that do not fit the declared income are a refund-fraud indicator.

## VAT Act 89 of 1991 and PAYE (employer obligations)

Source: https://www.sars.gov.za/legal-counsel/primary-legislation/ (Value-Added Tax Act; Fourth Schedule to the Income Tax Act)
Status: unverified

VAT: standard rate, zero-rated and exempt supplies, and a compulsory registration threshold on turnover. PAYE: employers deduct tax from remuneration and pay SARS monthly, issue employee tax certificates and reconcile. SDL and UIF contributions accompany payroll. Employer-side questions should quote the tax year and refer to the SARS employer guide.

## FICA: Financial Intelligence Centre Act 38 of 2001

Source: https://www.fic.gov.za
Status: unverified

Requires accountable institutions to identify and verify customers, keep records, and report to the FIC: suspicious and unusual transactions, cash above a prescribed threshold, and certain other reports. Reporting has deadlines, and tipping off the subject is prohibited. If the agent identifies activity that looks like money laundering, it should flag it for the institution's compliance or money laundering reporting officer. It must not contact the subject.

## POCA and PRECCA: organised crime, money laundering and corruption reporting

Source: https://www.gov.za/documents/acts (Prevention of Organised Crime Act 121 of 1998; Prevention and Combating of Corrupt Activities Act 12 of 2004)
Status: unverified

POCA criminalises money laundering and dealing with proceeds of unlawful activity. PRECCA places a duty on persons in positions of authority to report certain offences (including fraud, theft and corruption above a prescribed value) to the police. These duties sit with people, so the agent should escalate to the responsible human and not act alone.

## POPIA: Protection of Personal Information Act 4 of 2013

Source: https://inforegulator.org.za
Status: unverified

Personal information must be processed lawfully, for a specific purpose, minimised, kept secure, and breaches notified to the Information Regulator and affected people. Tax and fraud data is personal information. The agent should show only the fields needed for the task, avoid copying identifiers into prompts to third-party LLMs where it can, and log what it accessed.

## POPIA section 71: automated decision making

Source: https://inforegulator.org.za (POPIA section 71)
Status: unverified

A person must not be subject to a decision with legal consequences or substantial effect based solely on automated processing of personal information, subject to stated exceptions. Design consequence for this platform: the agent produces advice and evidence for a human decision. A flagged application must be reviewed by a person before any adverse action. Record who reviewed and why.

## Cross-border: sending personal data to external LLM providers

Source: https://inforegulator.org.za (POPIA section 72, transfers outside South Africa)
Status: unverified

Using OpenAI or HuggingFace endpoints means personal information may leave South Africa. Before sending real taxpayer data to those providers, confirm the legal basis and consider masking names, emails and ID numbers in prompts. Synthetic data used in the current build avoids the issue, so production use requires a review.

## Answering rules for the agent

Source: platform design
Status: internal

1. Cite the section and source for every legal claim. If no retrieved source supports a claim, say you could not find support.
2. Quote the tax year with any rate, threshold, penalty amount or deadline.
3. If a retrieved chunk is marked `Status: unverified`, tell the user the point should be confirmed against the linked source.
4. Never state that a named person committed fraud or evasion.
5. For legal outcomes, recommend consulting SARS, the FIC or a registered tax practitioner.
6. If the question is outside South African tax, fraud and compliance, say so.
