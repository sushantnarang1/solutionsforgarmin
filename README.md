# Garmin FIT Data Processing Platform

> Production-grade, cloud-native architecture for transforming the Garmin FIT SDK Tools repository from a manual CLI suite into a scalable, observable, multi-tenant data processing platform.

## Executive Summary

This blueprint transforms the Garmin FIT SDK Tools repository from a manual, debugging-focused CLI suite into a production-grade, cloud-native data processing platform.

By treating `FitCSVTool.jar`, `ActivityRepairTool.jar`, and `Profile.xlsx` as immutable core assets, the platform operationalizes them within an event-driven, multi-tenant architecture emphasizing:

* Developer self-service
* Automated schema governance
* SRE-grade reliability
* GitOps-driven CI/CD
* Containerized CLI execution
* Schema registry-backed validation
* Idempotent event processing
* Unified Internal Developer Platform (IDP)
* Full data lineage
* Backward compatibility
* Zero-downtime deployments

The result is a scalable and observable pipeline capable of ingesting, repairing, converting, and distributing FIT data at enterprise scale without replacing the existing Garmin tooling.

---

## System Architecture

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DEVELOPER EXPERIENCE LAYER                          │
│                                                                             │
│  [Backstage Portal] ←→ [REST/gRPC API Gateway] ←→ [CLI Wrapper]             │
│                                                                             │
│  Job Submission • Status Tracking • Version Catalog • SDK Documentation    │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                     AuthN/Z • Rate Limiting • Tenant Routing
                                │
┌───────────────────────────────▼─────────────────────────────────────────────┐
│                    CONTROL PLANE & ORCHESTRATION LAYER                      │
│                                                                             │
│  [Argo Workflows / Temporal] ←→ [Schema Registry] ←→ [Feature Flags]        │
│                                                                             │
│  Pipeline DAGs • Schema Versioning • Canary Rollouts • Idempotency Keys     │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                    Event Routing • Schema Validation • Triggers
                                │
┌───────────────────────────────▼─────────────────────────────────────────────┐
│                         EVENT BUS & STREAMING LAYER                          │
│                                                                             │
│  [Apache Kafka / Redpanda] ←→ [Dead Letter Queue] ←→ [CDC/Change Streams]  │
│                                                                             │
│  Async Ingestion • Retry Topics • Partitioned Processing • Exactly-Once     │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                File Upload • Schema Routing • Repair/Convert Triggers
                                │
┌───────────────────────────────▼─────────────────────────────────────────────┐
│                       PROCESSING LAYER (CONTAINERIZED CLI)                  │
│                                                                             │
│  [FitCSVTool.jar Pod] ←→ [ActivityRepairTool.jar Pod] ←→ [Resource Quotas] │
│                                                                             │
│  Stateless Workers • Graceful Shutdown • Health Probes • Multi-AZ           │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                  Validated FIT/CSV • Metadata • Lineage Records
                                │
┌───────────────────────────────▼─────────────────────────────────────────────┐
│                            STORAGE & DATA LAYER                             │
│                                                                             │
│  [Object Storage] ←→ [PostgreSQL] ←→ [Redis]                               │
│                                                                             │
│  Raw FIT • Processed CSV • Schema Versions • Job State • Tenant Isolation  │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                       Metrics • Logs • Traces • Alerts
                                │
┌───────────────────────────────▼─────────────────────────────────────────────┐
│                      OBSERVABILITY & SECURITY LAYER                         │
│                                                                             │
│  [OpenTelemetry] ←→ [Prometheus/Grafana] ←→ [Loki/ELK] ←→ [Vault/IAM]      │
│                                                                             │
│  SLI/SLO Tracking • Distributed Tracing • Secret Rotation • RBAC           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Components

| Layer                 | Responsibility                                                | Recommended Technology                    | Justification                                               |
| --------------------- | ------------------------------------------------------------- | ----------------------------------------- | ----------------------------------------------------------- |
| **Ingestion**         | Secure file upload, event envelope creation, tenant routing   | S3/GCS + EventBridge/Kafka                | Durable storage and decoupled event processing              |
| **Orchestration**     | DAG execution, state management, retry/DLQ routing            | Argo Workflows + Temporal                 | Declarative pipelines and fault-tolerant workflow execution |
| **Schema Governance** | Parse `Profile.xlsx`, generate schemas, enforce compatibility | Python/Java + Schema Registry             | Centralized schema lifecycle and compatibility enforcement  |
| **Processing**        | Containerize and execute JARs                                 | Docker + Kubernetes + KEDA                | Stateless execution, autoscaling, and resource isolation    |
| **Storage**           | Object storage, metadata, caching                             | S3/GCS + PostgreSQL + Redis               | Durable artifacts, ACID metadata, low-latency state         |
| **Control Plane**     | Developer portal, API gateway, version catalog                | Backstage + Kong/Envoy                    | Standard internal developer platform pattern                |
| **Observability**     | Metrics, logs, traces, alerting                               | OpenTelemetry + Prometheus/Grafana + Loki | Vendor-neutral, Kubernetes-native observability             |
| **Security**          | IAM, mTLS, secrets, tenant isolation                          | Vault + OIDC + Kubernetes RBAC            | Zero-trust security and automated secret rotation           |

---

# Ingestion & Processing Pipeline

The platform processes FIT files through the following workflow:

### 1. Ingestion & Envelope Creation

A developer or upstream system uploads a `.fit` file to tenant-scoped object storage.

An event envelope is published to the ingestion Kafka topic containing:

* File URI
* Tenant ID
* Idempotency key
* Schema version
* Upload metadata

### 2. Schema Validation Gate

A lightweight sidecar or pre-processing worker:

1. Retrieves the active schema version from the Schema Registry.
2. Loads constraints derived from `Profile.xlsx`.
3. Validates the FIT binary structure.
4. Rejects malformed files with structured error codes.
5. Routes rejected events to the Dead Letter Queue (DLQ).

### 3. Repair Routing

If validation detects corruption or repairable invalidation flags:

```text
FIT Input
   │
   ▼
Validation
   │
   ├── Valid ────────────────┐
   │                         │
   └── Repair Required       │
          │                  │
          ▼                  │
ActivityRepairTool.jar       │
          │                  │
          ├── Success ───────┤
          │                  │
          └── Failure → DLQ  │
                             ▼
                       Conversion
```

Repair outcomes are versioned and recorded for complete lineage tracking.

### 4. Conversion & Transformation

`FitCSVTool.jar` runs inside a resource-quota-enforced Kubernetes pod.

The worker:

* Converts FIT data to CSV.
* Streams generated artifacts to object storage.
* Records processing metadata.
* Emits completion events.

Metadata includes:

* Row counts
* Field mappings
* Processing duration
* Tool version
* Schema version
* Input/output artifact identifiers

### 5. Distribution & Lineage

Processed artifacts are published to tenant-specific storage locations.

The platform maintains lineage:

```text
Raw FIT
   │
   ▼
Validation
   │
   ▼
Repair
   │
   ▼
Conversion
   │
   ▼
Processed CSV
```

Downstream consumers can subscribe through:

* Kafka
* REST APIs
* Object-storage notifications

### 6. Idempotency & Retry

Every processing step uses deterministic idempotency keys.

Failures trigger:

```text
Processing Failure
       │
       ▼
Exponential Backoff
       │
       ├── Retry Successful ──→ Continue Pipeline
       │
       └── Retry Exhausted
                │
                ▼
               DLQ
                │
                ▼
        Manual / Automated Replay
```

This prevents duplicate processing and enables safe recovery.

---

# Platform Engineering Controls

## CI/CD & Infrastructure

### Infrastructure as Code

Use Terraform or Pulumi for:

* Kubernetes clusters
* Networking
* Storage
* IAM
* Kafka infrastructure
* Database infrastructure

Use ArgoCD for GitOps-based deployment.

### CI Pipeline

GitHub Actions or GitLab CI should automate:

1. JAR containerization
2. Unit and integration testing
3. Schema extraction
4. Schema compatibility testing
5. Container image creation
6. SBOM generation
7. Vulnerability scanning
8. Image signing
9. Artifact publishing

Recommended security tooling:

* Trivy
* Grype
* Cosign
* SBOM generation

### Artifact Management

Maintain:

* Signed container images
* Versioned JAR artifacts
* Versioned schemas
* FIT type definitions
* Immutable release tags

Semantic versioning should be enforced through CI.

### Environment Promotion

```text
Developer
    │
    ▼
   Dev
    │
    ▼
 Staging
    │
    ▼
 Production
```

Promotions should be GitOps-controlled and support:

* Feature flags
* Canary deployments
* Blue/green deployments
* Rolling updates
* Readiness probes
* Automatic rollback

---

# Observability & SRE

## Service-Level Indicators and Objectives

| SLI                      |                                Target |
| ------------------------ | ------------------------------------: |
| **Throughput**           |        ≥ 10,000 FIT files/hour/tenant |
| **Processing Latency**   |       P95 repair/convert < 45 seconds |
| **Pipeline Reliability** |           99.9% successful processing |
| **Schema Compliance**    | 100% validation for approved versions |

These targets should be validated against real production workloads during capacity planning rather than treated as guaranteed baseline capacity.

## Metrics

Prometheus should monitor:

* Kafka queue depth
* Processing throughput
* Processing latency
* Error rate
* Retry count
* DLQ volume
* Pod restarts
* CPU utilization
* Memory utilization
* Kubernetes scheduling failures
* Per-tenant workload

## Distributed Tracing

OpenTelemetry traces should span:

```text
Upload
  │
  ├── Validation
  │
  ├── Repair
  │
  ├── Conversion
  │
  ├── Object Storage
  │
  └── Metadata Database
```

Every trace should carry correlation information such as:

* Tenant ID
* Job ID
* Idempotency key
* Schema version
* Tool version

## Logging

Centralized logging can use:

* Loki
* Elasticsearch
* OpenSearch

Logs should use structured JSON and include correlation IDs.

Sensitive data must never be written to application logs.

## Alerting

Grafana Alerting / Alertmanager can route alerts to:

* PagerDuty
* Slack
* Incident-management systems

Prioritize SLO burn-rate alerts over noisy static thresholds.

## Failure Recovery

Recovery follows:

```text
Failure
  │
  ▼
Retry with Exponential Backoff
  │
  ├── Success → Continue
  │
  └── Exhausted
          │
          ▼
         DLQ
          │
          ▼
     Replay / Inspection
```

Idempotent writes and deduplication keys ensure replay does not create duplicate artifacts.

---

# Developer Self-Service

## REST/gRPC API

The platform should expose APIs for:

* FIT file submission
* Job status
* Job history
* Schema version listing
* Tool version listing
* Artifact retrieval
* Validation
* Job cancellation
* Job replay

Example API flow:

```text
POST /v1/jobs
        │
        ▼
   Job Created
        │
        ▼
GET /v1/jobs/{jobId}
        │
        ▼
  Job Status
        │
        ▼
GET /v1/jobs/{jobId}/artifacts
```

## CLI Wrapper

Provide a Go or Python CLI named:

```text
fit-platform-cli
```

Example commands:

```bash
fit-platform-cli upload activity.fit
fit-platform-cli validate activity.fit
fit-platform-cli status <job-id>
fit-platform-cli artifacts <job-id>
fit-platform-cli schema list
```

The CLI should support:

* Batch uploads
* Dry-run validation
* Local schema validation
* Job monitoring
* Artifact retrieval
* Explicit version selection

## Backstage Portal

The Backstage-based Internal Developer Platform should provide:

* Service catalog
* Job dashboard
* FIT processing submission
* Job history
* SDK/tool version matrix
* Schema catalog
* Runbooks
* Operational documentation
* Ownership information

---

# Schema Governance

`Profile.xlsx` should remain the authoritative source asset while being transformed into machine-readable schemas during CI.

```text
Profile.xlsx
     │
     ▼
Schema Extraction
     │
     ▼
Machine-Readable Schema
     │
     ▼
Compatibility Tests
     │
     ▼
Schema Registry
     │
     ▼
Production Validation
```

Schema changes should require:

1. Pull request review.
2. Automated extraction.
3. Compatibility testing.
4. Schema version assignment.
5. Consumer-impact analysis.
6. Promotion through environments.

Supported compatibility strategies should include:

* Backward compatibility
* Forward compatibility
* Full compatibility where required

---

# Security & Multi-Tenancy

The platform should use tenant-scoped isolation throughout the stack.

### Isolation Model

```text
Tenant
 ├── Kubernetes Namespace
 ├── Object Storage Prefix/Bucket
 ├── Kafka Topics/Partitions
 ├── IAM Permissions
 ├── Database Access Scope
 └── Audit Records
```

### Security Controls

* OIDC-based authentication
* Kubernetes RBAC
* Least-privilege IAM
* mTLS between services
* Vault-managed secrets
* Automatic secret rotation
* Tenant-scoped storage
* Tenant-scoped queues
* Immutable audit logs
* Network policies
* Encryption at rest
* Encryption in transit

The architecture should follow zero-trust principles.

---

# Implementation Phases

| Phase                             | Scope                                                                              | Deliverables                                                        | Success Criteria                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| **1. Foundation**                 | Containerize JARs, establish CI/CD, extract schemas, create single-tenant pipeline | Docker images, Terraform foundation, Schema Registry, Argo workflow | JARs run statelessly in Kubernetes; schemas are versioned; manual uploads succeed |
| **2. Automation & Observability** | Event bus, DLQ/retry, OpenTelemetry, SLOs, metadata DB                             | Kafka pipeline, dashboards, tracing, idempotency                    | ≥99.5% initial success target; full traceability; automated alerts                |
| **3. Self-Service & Scale**       | API gateway, CLI, Backstage, multi-tenant isolation                                | REST/gRPC APIs, `fit-platform-cli`, tenant routing, RBAC            | Developers submit and track jobs without platform-team intervention               |
| **4. Governance & Optimization**  | Lineage, schema evolution, cost controls, streaming                                | Audit lineage, compatibility gates, KEDA autoscaling                | Zero-downtime schema updates; cost-per-file visibility; production streaming      |

---

# Migration Strategy

Migration should use a **parallel-run strategy** with feature flags.

```text
                    ┌───────────────┐
                    │ Legacy CLI    │
                    └───────┬───────┘
                            │
                       Parallel Run
                            │
                    ┌───────▼───────┐
                    │ New Platform  │
                    └───────┬───────┘
                            │
                     Output Comparison
                            │
                    ┌───────▼───────┐
                    │ Parity Checks │
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │ Cutover       │
                    └───────────────┘
```

During migration:

* Keep the legacy CLI available.
* Run legacy and automated pipelines in parallel.
* Compare outputs automatically.
* Validate schema parity.
* Maintain rollback procedures.
* Test rollback procedures in staging.
* Gradually increase production traffic to the new platform.

The legacy tooling should only be retired after output parity and operational readiness have been demonstrated.

---

# Risks & Mitigation Strategies

| Risk                             | Impact                                  | Mitigation                                                                                     |
| -------------------------------- | --------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `Profile.xlsx` structure changes | Schema mismatch and validation failures | Automated Excel-to-schema parser, CI validation, Schema Registry compatibility gates           |
| Large/corrupted FIT files        | OOM errors and pipeline stalls          | Memory limits, streaming/chunked processing where supported, graceful degradation, DLQ routing |
| JAR version incompatibility      | Broken conversions                      | Immutable image tags, version pinning, automated compatibility tests                           |
| Multi-tenant data leakage        | Compliance and security violation       | Namespace isolation, tenant-scoped queues/storage, strict RBAC, audit logging                  |
| Breaking schema evolution        | Downstream pipeline failures            | Compatibility checks, versioned consumers, canary schema rollout                               |
| High-concurrency spikes          | Queue backlog and latency degradation   | KEDA autoscaling, Kafka partitioning, rate limiting, asynchronous processing                   |
| External dependency failure      | Processing delays                       | Timeouts, retries, circuit breakers, DLQs, dependency health checks                            |
| Duplicate event delivery         | Duplicate processing                    | Deterministic idempotency keys and deduplication                                               |
| Worker termination               | Partial or inconsistent jobs            | Graceful shutdown, checkpointing where supported, retryable job state                          |

---

# Reference Technology Stack

| Capability             | Technology                       |
| ---------------------- | -------------------------------- |
| Container Runtime      | Docker                           |
| Orchestration          | Kubernetes                       |
| Autoscaling            | KEDA                             |
| Workflow Orchestration | Argo Workflows / Temporal        |
| Event Streaming        | Apache Kafka / Redpanda          |
| Object Storage         | Amazon S3 / Google Cloud Storage |
| Metadata Database      | PostgreSQL                       |
| Cache                  | Redis                            |
| Schema Registry        | Confluent Schema Registry        |
| Developer Portal       | Backstage                        |
| API Gateway            | Kong / Envoy                     |
| Infrastructure as Code | Terraform / Pulumi               |
| GitOps                 | ArgoCD                           |
| CI/CD                  | GitHub Actions / GitLab CI       |
| Observability          | OpenTelemetry                    |
| Metrics                | Prometheus                       |
| Dashboards             | Grafana                          |
| Logs                   | Loki / ELK                       |
| Secrets                | HashiCorp Vault                  |
| Authentication         | OIDC                             |
| Security Scanning      | Trivy / Grype                    |
| Image Signing          | Cosign                           |

---

# Repository Structure

A potential repository structure for the platform could be:

```text
.
├── README.md
├── docs/
│   ├── architecture.md
│   ├── operations.md
│   ├── security.md
│   ├── schema-governance.md
│   └── migration.md
│
├── tools/
│   ├── FitCSVTool.jar
│   ├── ActivityRepairTool.jar
│   └── Profile.xlsx
│
├── schemas/
│   ├── generated/
│   └── compatibility/
│
├── services/
│   ├── ingestion/
│   ├── validation/
│   ├── orchestration/
│   └── metadata/
│
├── workers/
│   ├── fit-csv/
│   └── activity-repair/
│
├── cli/
│   └── fit-platform-cli/
│
├── infrastructure/
│   ├── terraform/
│   ├── kubernetes/
│   └── argocd/
│
├── observability/
│   ├── prometheus/
│   ├── grafana/
│   └── otel/
│
└── .github/
    └── workflows/
```

---

# Production Readiness Checklist

* [ ] Containerize `FitCSVTool.jar`
* [ ] Containerize `ActivityRepairTool.jar`
* [ ] Automate `Profile.xlsx` schema extraction
* [ ] Establish schema compatibility testing
* [ ] Deploy Schema Registry
* [ ] Deploy Kubernetes processing workers
* [ ] Implement event-driven ingestion
* [ ] Implement deterministic idempotency
* [ ] Implement retry and DLQ handling
* [ ] Establish metadata database
* [ ] Implement complete data lineage
* [ ] Deploy OpenTelemetry instrumentation
* [ ] Create Prometheus/Grafana dashboards
* [ ] Configure SLO-based alerting
* [ ] Implement tenant isolation
* [ ] Configure OIDC and RBAC
* [ ] Integrate Vault for secrets
* [ ] Build REST/gRPC APIs
* [ ] Build `fit-platform-cli`
* [ ] Create Backstage developer portal
* [ ] Establish GitOps deployment
* [ ] Implement canary/blue-green deployments
* [ ] Establish automated rollback
* [ ] Perform load testing
* [ ] Perform failure-injection testing
* [ ] Validate legacy/new output parity
* [ ] Document operational runbooks

---

# Hiring-Ready Value Proposition

This architecture demonstrates mature platform engineering discipline by transforming legacy, manual CLI utilities into a governed, self-service, and SRE-grade data platform without replacing the core Garmin assets.

It demonstrates expertise across:

* Cloud-native architecture
* Platform engineering
* Kubernetes
* Event-driven systems
* Schema lifecycle management
* Developer experience
* GitOps
* Infrastructure as Code
* Observability
* SRE practices
* Multi-tenant architecture
* Data lineage
* Fault-tolerant processing
* Zero-downtime deployments

The phased migration strategy reduces operational risk while preserving backward compatibility with the existing tooling.

The resulting platform reduces manual toil, accelerates developer onboarding, provides strong operational visibility, and establishes a scalable foundation for enterprise FIT data processing.

---

# Key Architectural Principles

1. **Preserve Core Assets**
   `FitCSVTool.jar`, `ActivityRepairTool.jar`, and `Profile.xlsx` remain immutable source assets.

2. **Automate Around the Tools**
   The platform adds orchestration, validation, observability, and governance without unnecessarily rewriting proven processing logic.

3. **Everything Is Versioned**
   Tool versions, schema versions, workflows, containers, and infrastructure are explicitly versioned.

4. **Everything Is Idempotent**
   Duplicate events must not result in duplicate processing or artifacts.

5. **Everything Is Observable**
   Every job should be traceable from ingestion through final artifact delivery.

6. **Everything Is Tenant-Aware**
   Storage, queues, permissions, metadata, and processing must respect tenant boundaries.

7. **Fail Safely**
   Retry transient failures, isolate permanent failures in the DLQ, and provide controlled replay.

8. **Deploy Without Downtime**
   Use readiness probes, rolling/canary deployments, feature flags, and automated rollback.

9. **Govern Schema Changes**
   No schema reaches production without automated compatibility validation.

10. **Enable Developer Self-Service**
    Developers should be able to submit, monitor, validate, and retrieve FIT processing jobs without direct platform-team intervention.
