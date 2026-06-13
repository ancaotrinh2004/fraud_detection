# Engineering for Data and AI - Coursework Proposal

## 1. Objective

Each group (num. of students = 1) will design and implement an end-to-end Data + AI system in one domain (for example: e-commerce, finance, logistics, healthcare, education, IoT).

Your submission must include both:
- design documents (.md)
- runnable implementation artifacts (scripts/services/jobs + evidence)

Reference examples are provided in [coursework/sample_design](coursework/sample_design).

---

## 2. Coursework Structure (Current)

Your work should follow this section structure:

1. `01_data_generator.md`
- design offline and streaming source datasets
- define schema, grain, and generation controls
- inject realistic data challenges

2. `02_schema_design_example.md`
- design storage/schema and pipeline contracts
- define data quality checks, SLA, update policy, and backfill strategy
- define business-ready serving tables/views and naming conventions

3. `03_data_generator_improvement.md`
- improve generator realism and controls
- add drift/change scenarios and track-specific data impact
- justify why the new scenarios matter for downstream AI behavior

4. Choose one AI track:
- `04.1_ml_design_example.md` (ML system design + implementation), or
- `04.2_llm_design.md` (LLM system design + implementation)

Note: You may complete both AI tracks for learning, but at least one is required for grading.

---

## 3. Core Requirements Across All Sections

Every section should be implementation-oriented, not only conceptual.

Required coverage:
- assumptions and scope boundaries
- explicit inputs/outputs and data contracts
- failure handling and recovery approach
- observability (logs, metrics, traces)
- security basics (auth, RBAC, secrets, sensitive data handling)
- CI/CD expectations for services and pipelines in scope

---

## 4. Data Engineering Requirements

Minimum requirements for generated and transformed data:
- offline + streaming data paths must both exist
- timestamps must be clearly defined and used consistently (`event_timestamp`, `created_ts`)
- point-in-time correctness must be considered where relevant
- schema evolution/update behavior must be defined
- at least one drift/change scenario must be designed and demonstrated

Recommended challenge types:
- skew/high-cardinality joins
- bursty traffic/late arrivals/out-of-order events
- duplicates/missing values/inconsistent formats

---

## 5. AI Track Expectations

### A. ML Track (`04.1_ml_design_example.md`)
- include high-level and low-level design
- define label/training data contract and split policy
- implement training, inference path, monitoring, and retraining policy
- define model/version rollout and rollback approach

### B. LLM Track (`04.2_llm_design_example.md`)
- include high-level and low-level design
- define knowledge sources, indexing, retrieval, and tool-call contract
- define serving, safety, evaluation, and reindexing policy
- define application log storage design (exclude system logs)

---

## 6. Deliverables

Submit:
- one markdown design document per completed section
- runnable code for each completed section
- sample outputs (tables/files/log snippets/screenshots)
- short run instructions for reproducibility

Minimum evidence checklist:
1. design decisions and trade-offs are explicit
2. schema/tables/pipelines/services are identifiable and testable
3. monitoring and alert conditions are stated
4. CI/CD intent is clear for implemented components
5. drift or behavior-change scenario is demonstrated

---

## 7. Phasing

1. Mini-coursework phase
- complete Section 01 and Section 02 with code evidence

2. Final coursework phase
- complete Section 03
- complete one AI track (Section 04.1 or Section 04.2)

This phasing ensures the data foundation is stable before AI system design and operation.