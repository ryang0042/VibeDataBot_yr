export type AgentStatus = "IDLE" | "THINKING" | "PLANNING" | "EXECUTING" | "PAUSED" | "DONE" | "ERROR";

export type StepType =
    | "LOAD_DATA"
    | "INSPECT_SCHEMA"
    | "SCAN_PII"
    | "TRANSFORM"
    | "GENERATE_SYNTHETIC"
    | "WRITE_DATA"
    | "EXTRACT_PDF"
    | "CLEAN_TEXT"
    | "DEDUPLICATE"
    | "QUALITY_CHECK"
    | "GENERATE_CORPUS";

export interface JobStep {
    id: string;
    type: StepType;
    label: string;
    description: string;
    status: "pending" | "running" | "completed" | "failed";
    codeSnippet?: string; // Python code representation
    metadata?: Record<string, unknown>;
}

export interface ExecutionPlan {
    id: string;
    goal: string;
    steps: JobStep[];
    createdAt: number;
    artifacts?: Record<string, unknown[]>; // stepId -> data rows
}

export interface ExecutionProgress {
    stepId: string | null;
    stepLabel: string;
    message: string;
    percent: number;
    indeterminate?: boolean;
}

export interface AgentMessage {
    id: string;
    sender: "user" | "agent";
    content: string;
    timestamp: number;
    relatedPlanId?: string;
}
