#!/usr/bin/env python3
"""
Generate 20 realistic enterprise documents (10 PDF + 10 DOCX) for batch testing
the Local RAG pipeline and triggering KEDA autoscaling.
"""

import os
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
import docx

OUTPUT_DIR = "/home/het/rag/batch_test_docs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

styles = getSampleStyleSheet()
title_style = styles["Title"]
heading_style = styles["Heading2"]
normal_style = styles["Normal"]

# -------------------------------------------------------------
# 10 PDF Documents
# -------------------------------------------------------------
pdf_docs = [
    {
        "filename": "Aegis_Hyperion_Cold_Gas_Propulsion_v4.pdf",
        "title": "Aegis Hyperion Cold-Gas Plasma Propulsion Engine Specification",
        "sections": [
            ("1. System Overview & Mission Architecture",
             "Project Hyperion represents Aegis Dynamics' next-generation deep-space autonomous satellite maneuvering thruster. "
             "The engine combines high-efficiency cold-gas expansion nozzles with pulse-inductive plasma pre-heating stages. "
             "The thruster is specifically rated for 15-year orbital station-keeping missions in geosynchronous transfer orbits."),
            ("2. Proprietary Propellant & Chamber Specifications",
             "The Hyperion engine utilizes a proprietary propellant mixture designated Xenon-9 (XN-9). "
             "Xenon-9 consists of ultra-high-purity xenon (99.999%) chemically enriched with 4.5% krypton-stabilized argon carrier gas. "
             "Under nominal firing conditions, the thruster operates at a chamber pressure of 4,250 PSI (pounds per square inch). "
             "The nominal mass flow rate is calibrated strictly at 12.8 grams per second (g/s), yielding a measured specific impulse (Isp) of 315 seconds. "
             "Propellant storage manifolds must maintain a cryogenic temperature of -40 degrees Celsius prior to nozzle induction."),
            ("3. Thermal Barrier and Material Durability",
             "The expansion bell is lined with a dual-phase zirconia-yttria ceramic matrix capable of withstanding peak plasma plume temperatures of 2,100 degrees Celsius. "
             "Structural telemetry sensors monitor injector throat erosion at 500 Hz sampling intervals to avoid thermal runaway.")
        ]
    },
    {
        "filename": "Aegis_Executive_Compensation_Policy_2026.pdf",
        "title": "Aegis Dynamics Executive Compensation, Bonus, and Severance Policy 2026",
        "sections": [
            ("1. Executive Retention Bonuses",
             "All senior vice presidents and c-suite executives are eligible for performance bonuses disbursed quarterly on the final business day of March, June, September, and December. "
             "Disbursements require dual sign-off from Chief Financial Officer Marcus Sterling (Email: marcus.sterling@aegis-dynamics.internal, SSN: 981-22-3344) and HR Director Sarah Vance."),
            ("2. Severance Packages & Vested Equity",
             "Executives departing after more than 24 months of continuous service receive a standard severance package comprising 9 months of base compensation and immediate vesting of Tier-2 stock options. "
             "All severance payments are issued on Net 15 business cycles via corporate treasury wire.")
        ]
    },
    {
        "filename": "Aegis_Quantum_Transmon_Computing_Roadmap.pdf",
        "title": "Aegis Quantum Systems: 128-Qubit Superconducting Architecture",
        "sections": [
            ("1. Quantum Processor Topology",
             "The Aegis Q-128 processor utilizes a 2D heavy-hexagonal lattice of transmon qubits fabricated from epitaxial niobium aluminum oxide thin films. "
             "The processor operates inside a dilution refrigerator at a base temperature of 14 millikelvin (mK)."),
            ("2. Quantum Relaxation Times and Gate Error Rates",
             "The targeted median T1 relaxation time is 185 microseconds, and median T2 dephasing time is 140 microseconds. "
             "Single-qubit randomized benchmarking yields an average error rate of 0.08%, while cross-resonance two-qubit CNOT gates achieve 0.35% infidelity.")
        ]
    },
    {
        "filename": "Aegis_HR_Global_Travel_And_Per_Diem.pdf",
        "title": "Aegis Global Business Travel, Meal Stipends, and Lodging Policy",
        "sections": [
            ("1. Flight Class & Accommodations",
             "Economy class is mandatory for domestic flights under 5 hours. Business class booking is permitted for continuous international travel exceeding 7 hours. "
             "Hotel lodging is capped at $280 per night in standard tier cities and $380 per night in high-cost metropolitan areas (New York, London, Tokyo, Zurich)."),
            ("2. Daily Per Diem Allowances",
             "Employees are allocated a daily per diem of $95 for meals and incidental expenses. Receipts are required for any single meal expense exceeding $40. "
             "Alcoholic beverages may not be expensed unless directly associated with approved business development entertainment.")
        ]
    },
    {
        "filename": "Aegis_AI_Model_Governance_And_Risk.pdf",
        "title": "Aegis AI Safety, Automated Decisioning, and Model Governance Standard",
        "sections": [
            ("1. AI Risk Tiering Framework",
             "All machine learning models deployed in production must be classified under Tier 1 (Mission-Critical / Flight Systems), Tier 2 (Operational / Analytics), or Tier 3 (Internal Productivity). "
             "Tier 1 models require full adversarial red-teaming, determinism audits, and board-level risk acceptance prior to release."),
            ("2. Hallucination Thresholds and Grounding Standards",
             "Generative AI applications serving customer telemetry or safety-critical engineering data must achieve a minimum citation faithfulness score of 98.5%. "
             "Any model generating ungrounded assertions exceeding 1.5% in regression benchmarks must be instantly gated by the automated CI/CD pipeline.")
        ]
    },
    {
        "filename": "Aegis_Cloud_Infrastructure_Cost_Budgets_2026.pdf",
        "title": "Aegis Infrastructure Engineering: Multi-Cloud Budget Allocation & FinOps 2026",
        "sections": [
            ("1. Cloud Compute Quotas",
             "The annual engineering compute budget for cloud GPU clusters is capped at $4.2M for FY2026. "
             "All development GPU instances (A100/H100) must implement auto-shutdown policies idling after 30 minutes of inactivity."),
            ("2. Storage Tiering & Lifecycle Management",
             "Telemetry logs older than 90 days must be migrated automatically to cold archive tier (Glacier-equivalent) with expected retrieval latency of 3-5 hours. "
             "Uncompressed vector embeddings older than 180 days must be quantized to int8 format to minimize memory footprint.")
        ]
    },
    {
        "filename": "Aegis_Cryptographic_Key_Rotation_Schedule.pdf",
        "title": "Aegis Information Security: PKI Infrastructure & Key Lifecycle",
        "sections": [
            ("1. Root CA and Intermediate Key Hierarchy",
             "The Aegis Root Certificate Authority is stored in an offline Hardware Security Module (HSM) located in a dual-custody air-gapped facility. "
             "Intermediate issuing CAs maintain an active validity window of 3 years and must be renewed 180 days prior to expiration."),
            ("2. Mandatory Asymmetric Key Rotation Periods",
             "TLS session certificates and internal microservice mTLS keys must rotate automatically every 45 days. "
             "RSA-4096 and ECDSA P-384 tenant signing keys must undergo cryptographic shredding upon scheduled decommissioning.")
        ]
    },
    {
        "filename": "Aegis_Autonomous_Drone_Fleet_Telemetry.pdf",
        "title": "Aegis SkySentinel Autonomous Drone Fleet: Communications & Mesh Telemetry",
        "sections": [
            ("1. Mesh Radio Frequency Allocations",
             "SkySentinel long-range reconnaissance UAVs communicate across encrypted COFDM datalinks operating within the 2.412–2.484 GHz and 5.725–5.850 GHz ISM bands. "
             "Each drone serves as an active hopping relay, supporting up to 32 simultaneous node connections."),
            ("2. Failsafe Return-to-Base (RTB) Protocols",
             "Loss of command telemetry link exceeding 12 seconds automatically triggers autonomous GPS-denied inertial navigation RTB mode. "
             "Drones descend to a safe cruising altitude of 150 meters AGL and approach the designated home beacon at 22 m/s.")
        ]
    },
    {
        "filename": "Aegis_Corporate_Environmental_ESG_2026.pdf",
        "title": "Aegis Dynamics Environmental Sustainability, Clean Energy, and Carbon Reduction",
        "sections": [
            ("1. Scope 1 and Scope 2 Emissions Targets",
             "Aegis Dynamics commits to achieving net-zero greenhouse gas emissions across all corporate campuses and manufacturing facilities by 2032. "
             "Solar array installations at the Mojave Spaceport currently provide 68% of baseline daytime manufacturing electricity."),
            ("2. Hazardous Waste Neutralization",
             "Solvents and chemical propellants from testing labs must undergo on-site supercritical water oxidation (SCWO) neutralization. "
             "Zero untreated volatile chemical compounds may be released into regional sewer networks.")
        ]
    },
    {
        "filename": "Aegis_Software_Release_CI_CD_Quality_Gates.pdf",
        "title": "Aegis Aerospace Software Engineering: Flight Code Quality & Release Gates",
        "sections": [
            ("1. Static Analysis and MISRA C Compliance",
             "All flight control code written in C/C++ must achieve 100% compliance with MISRA C:2012 guidelines. "
             "Zero critical warnings from Coverity or SonarQube are permitted in merge requests targeting the main release branch."),
            ("2. Integration Test Coverage and Hardware-in-the-Loop",
             "Software pull requests must demonstrate minimum 92% statement coverage and 85% modified condition/decision coverage (MC/DC). "
             "Nightly hardware-in-the-loop (HIL) simulators must run 1,000 automated flight scenario tests with 0 failures before flight qualification tagging.")
        ]
    }
]

# -------------------------------------------------------------
# 10 Word (.docx) Documents
# -------------------------------------------------------------
docx_docs = [
    {
        "filename": "Aegis_Hyperion_Emergency_Protocols_2026.docx",
        "title": "Aegis Hyperion Emergency Shutdown and Fail-Safe Procedures 2026",
        "sections": [
            ("1. Emergency Shutdown Trigger Codes",
             "The Hyperion plasma propulsion engine is governed by three fail-safe threshold codes: Code Yellow-2 (Telemetry Anomaly), Code Orange-5 (Pressure Surge > 4,600 PSI), and Code Amber-7 (Critical Chamber Overpressure > 4,800 PSI)."),
            ("2. Code Amber-7 Dual-Key Biometric Protocol",
             "Execution of Code Amber-7 requires simultaneous dual optical-biometric authorization from Lead Propulsion Engineer Chen and Chief Safety Officer Vance. "
             "Both authorizations must be submitted to the mission control console within a strict 45-second window from initial acoustic alarm trigger. "
             "Once verified, automated hypergolic vent valve #3 opens, venting chamber gases through an emergency nitrogen sweep line to neutralize explosive mixture risks."),
            ("3. Post-Shutdown Telemetry Analysis",
             "Following an emergency shutdown, the propellant isolation solenoid locks in the normally-closed position. "
             "Engineers must inspect the downstream mass flow meter and verify zero Xenon-9 residual leakage for 4 consecutive hours prior to manual latch reset.")
        ]
    },
    {
        "filename": "Aegis_Cybersecurity_ZeroTrust_Compliance.docx",
        "title": "Aegis Corporate IT: Zero Trust Security Architecture & Identity Policy",
        "sections": [
            ("1. Zero Trust Principles and Continuous Verification",
             "No network location—internal or external—is implicitly trusted. All access requests must authenticate through phishing-resistant FIDO2 hardware security keys. "
             "Contextual risk engines evaluate user device posture, geographic coordinates, and time-of-day before granting session tokens."),
            ("2. Privileged Access Management (PAM)",
             "Administrative access to production Kubernetes clusters and core databases requires just-in-time (JIT) elevation with automated expiration after 60 minutes. "
             "All administrative keystrokes and shell commands are cryptographically hashed and mirrored to an immutable audit store.")
        ]
    },
    {
        "filename": "Aegis_Supply_Chain_Vendor_Agreements.docx",
        "title": "Aegis Global Procurement: Approved Vendor Requirements and SLA Terms",
        "sections": [
            ("1. Vendor Prequalification Criteria",
             "Suppliers delivering aerospace-grade titanium, refractory alloys, or radiation-hardened electronics must hold valid AS9100 Rev D certifications. "
             "Aegis reserves the right to perform unannounced on-site quality audits with 48 hours prior notice."),
            ("2. Invoicing, Payment Terms, and Discounts",
             "Standard vendor billing follows Net 30 payment terms from certified receipt of goods. "
             "A mandatory 15% early settlement discount applies to all cryogenic component purchases above $250,000 paid within 10 business days.")
        ]
    },
    {
        "filename": "Aegis_Orbital_Debris_Mitigation_Standard.docx",
        "title": "Aegis Space Operations: Orbital Debris Mitigation and End-of-Life Disposal",
        "sections": [
            ("1. Post-Mission Disposal Timeframe",
             "In accordance with updated international space flight standards, all Aegis satellite platforms in low Earth orbit (LEO) must execute de-orbit maneuvers within 5 years of mission termination. "
             "Satellites in geostationary orbit must be boosted to a graveyard orbit at least 300 kilometers above the geostationary belt."),
            ("2. Passivation of Energy Sources",
             "Prior to de-orbit, onboard lithium-ion battery arrays must be discharged to zero stored chemical potential. "
             "Residual Xenon and nitrogen pressurant tanks must be vented down to less than 15 PSI to prevent catastrophic fragmentation.")
        ]
    },
    {
        "filename": "Aegis_Facility_Security_Badge_Access_Rules.docx",
        "title": "Aegis Physical Security: Access Control, Visitor Protocols, and Facility Badges",
        "sections": [
            ("1. Facility Security Zones",
             "Aegis corporate and laboratory campuses are segmented into Green (General Office), Blue (Engineering Labs), and Red (Classified Propulsion R&D). "
             "Red Zone entry requires an active Top Secret security clearance, dual-factor biometric retina scan, and escort logging."),
            ("2. Visitor Escort Regulations",
             "Visitors must be badged at the main gate and remain under visual line-of-sight escort by an Aegis employee at all times. "
             "Recording devices, including cameras, smartwatches, and USB flash drives, must be secured in entrance lockers.")
        ]
    },
    {
        "filename": "Aegis_Data_Classification_And_DLP_SOP.docx",
        "title": "Aegis Data Classification, Information Rights Management, and DLP Standard",
        "sections": [
            ("1. Four-Tier Classification Schema",
             "All digital documents, emails, and code repositories must carry one of four metadata classification tags: Public, Internal, Confidential, or Restricted Secret. "
             "Restricted Secret documents may never be synchronized to non-company mobile devices or unapproved cloud drives."),
            ("2. Data Loss Prevention (DLP) Enforcements",
             "Automated DLP agents monitor outbound email gateways and USB endpoints. "
             "Transfers containing regex patterns matching Social Security Numbers, credit cards, or proprietary aerospace design drawings are blocked instantly and trigger an automated SOC incident ticket.")
        ]
    },
    {
        "filename": "Aegis_Customer_SLA_And_Disaster_Recovery.docx",
        "title": "Aegis Enterprise Cloud Services: Service Level Agreements and DR Playbook",
        "sections": [
            ("1. Platform Availability Guarantees",
             "Aegis Cloud telemetry APIs guarantee 99.95% monthly uptime. "
             "Downtime exceeding 0.05% in any calendar month entitles enterprise clients to a 10% billing credit per each full hour of outage."),
            ("2. Recovery Objectives (RPO & RTO)",
             "The Recovery Point Objective (RPO) for flight operations data is 0 seconds (synchronous multi-region replication). "
             "The Recovery Time Objective (RTO) for full failover to secondary disaster recovery clusters is strictly under 4 minutes.")
        ]
    },
    {
        "filename": "Aegis_Intellectual_Property_Patents_Filing.docx",
        "title": "Aegis Legal Department: IP Assignment, Patent Disclosures, and Trade Secrets",
        "sections": [
            ("1. Invention Disclosure Submissions",
             "Engineering staff who develop novel mechanical apparatuses, algorithms, or chemical compositions must file an Invention Disclosure Form (IDF) within 30 days of reduction to practice. "
             "Patent review committees meet bi-weekly to assess commercial potential and patentability."),
            ("2. Inventor Incentive Compensation",
             "Primary inventors receive a $2,500 cash award upon formal submission to the USPTO and an additional $5,000 cash bonus upon patent grant. "
             "All proprietary rights and assigns remain the exclusive property of Aegis Dynamics Corporation.")
        ]
    },
    {
        "filename": "Aegis_Employee_Health_Benefits_Guide.docx",
        "title": "Aegis Employee Wellbeing: Comprehensive Healthcare, Dental, and Mental Health",
        "sections": [
            ("1. Health Insurance Plans",
             "Employees may select between a High-Deductible Health Plan (HDHP) with an employer-funded Health Savings Account (HSA) of $1,500/year or a Platinum PPO plan. "
             "Aegis covers 90% of monthly employee premiums and 75% of dependent premiums."),
            ("2. Mental Health & Wellness Benefits",
             "Employees and immediate family members have access to 12 complimentary annual therapy sessions through the Employee Assistance Program (EAP). "
             "Gym and wellness memberships are subsidized up to $80 per month.")
        ]
    },
    {
        "filename": "Aegis_Board_Of_Directors_Resolution_Q4.docx",
        "title": "Aegis Dynamics: Board of Directors Resolution & Strategic Directives Q4 2026",
        "sections": [
            ("1. Approval of Next-Generation Space Systems Budget",
             "The Board of Directors hereby unanimously resolves to authorize $85,000,000 in capital expenditures for the expansion of the Mojave Plasma Propulsion Integration Facility. "
             "Construction of vacuum test chamber #4 is scheduled to commence in January 2027."),
            ("2. Dividend Declaration and Share Repurchase",
             "A quarterly cash dividend of $0.42 per share is declared for shareholders of record as of November 30, 2026. "
             "The Board also authorizes a $100M open-market share buyback program over the next 18 months.")
        ]
    }
]

def generate_pdf(doc_info):
    filepath = os.path.join(OUTPUT_DIR, doc_info["filename"])
    doc = SimpleDocTemplate(filepath, pagesize=letter, rightMargin=54, leftMargin=54, topMargin=54, bottomMargin=54)
    story = []
    
    # Title
    story.append(Paragraph(doc_info["title"], title_style))
    story.append(Spacer(1, 14))
    
    # Sections
    for sec_title, sec_body in doc_info["sections"]:
        story.append(Paragraph(sec_title, heading_style))
        story.append(Spacer(1, 6))
        story.append(Paragraph(sec_body, normal_style))
        story.append(Spacer(1, 12))
        
    doc.build(story)
    print(f"Generated PDF: {filepath}")

def generate_docx(doc_info):
    filepath = os.path.join(OUTPUT_DIR, doc_info["filename"])
    doc = docx.Document()
    
    # Title
    doc.add_heading(doc_info["title"], 0)
    
    # Sections
    for sec_title, sec_body in doc_info["sections"]:
        doc.add_heading(sec_title, level=1)
        doc.add_paragraph(sec_body)
        
    doc.save(filepath)
    print(f"Generated DOCX: {filepath}")

if __name__ == "__main__":
    print("Generating 10 PDF documents...")
    for p in pdf_docs:
        generate_pdf(p)
        
    print("\nGenerating 10 Word (.docx) documents...")
    for d in docx_docs:
        generate_docx(d)
        
    print(f"\nAll 20 test documents successfully generated in: {OUTPUT_DIR}")
