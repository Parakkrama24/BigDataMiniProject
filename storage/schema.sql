-- Member 3 serving schema. PostgreSQL applies this file on first container start.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS patients (
    patient_id TEXT PRIMARY KEY,
    ward TEXT NOT NULL,
    admit_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS vitals_live (
    patient_id TEXT NOT NULL REFERENCES patients(patient_id),
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    avg_hr DOUBLE PRECISION,
    min_hr DOUBLE PRECISION,
    max_hr DOUBLE PRECISION,
    avg_spo2 DOUBLE PRECISION,
    min_spo2 DOUBLE PRECISION,
    max_spo2 DOUBLE PRECISION,
    avg_systolic_bp DOUBLE PRECISION,
    avg_diastolic_bp DOUBLE PRECISION,
    avg_temp DOUBLE PRECISION,
    event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0),
    anomaly_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (patient_id, window_start),
    CHECK (window_end > window_start)
);

CREATE TABLE IF NOT EXISTS lab_results (
    patient_id TEXT NOT NULL REFERENCES patients(patient_id),
    test_type TEXT NOT NULL,
    result_value DOUBLE PRECISION NOT NULL,
    reference_range TEXT NOT NULL,
    reference_low DOUBLE PRECISION NOT NULL,
    reference_high DOUBLE PRECISION NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    is_abnormal BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (patient_id, test_type, collected_at),
    CHECK (reference_low <= reference_high)
);

CREATE TABLE IF NOT EXISTS daily_risk_report (
    patient_id TEXT NOT NULL REFERENCES patients(patient_id),
    report_date DATE NOT NULL,
    vitals_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    lab_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    risk_score NUMERIC(5,2) NOT NULL CHECK (risk_score >= 0 AND risk_score <= 100),
    risk_flag TEXT NOT NULL CHECK (risk_flag IN ('LOW', 'MEDIUM', 'HIGH')),
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, report_date)
);

CREATE TABLE IF NOT EXISTS alerts_log (
    alert_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_id TEXT NOT NULL REFERENCES patients(patient_id),
    alert_type TEXT NOT NULL CHECK (alert_type IN ('TACHYCARDIA', 'HYPOXIA', 'FEVER', 'HYPOTENSION')),
    triggered_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    CHECK (resolved_at IS NULL OR resolved_at >= triggered_at)
);

CREATE INDEX IF NOT EXISTS idx_vitals_live_window ON vitals_live (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_lab_results_patient_collected ON lab_results (patient_id, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_patient_triggered ON alerts_log (patient_id, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_daily_risk_report_date_score ON daily_risk_report (report_date, risk_score DESC);
