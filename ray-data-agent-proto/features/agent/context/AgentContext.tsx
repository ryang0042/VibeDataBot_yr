"use client";

import React, { createContext, useContext, useState, ReactNode } from "react";
import { AgentStatus, ExecutionPlan, AgentMessage, ExecutionProgress } from "../types/AgentTypes";

interface AgentContextType {
    status: AgentStatus;
    setStatus: (status: AgentStatus) => void;

    messages: AgentMessage[];
    addMessage: (msg: AgentMessage) => void;

    logs: string[];
    addLog: (log: string) => void;

    plan: ExecutionPlan | null;
    setPlan: (plan: ExecutionPlan | null) => void;
    updateStepStatus: (stepId: string, status: ExecutionPlan["steps"][0]["status"]) => void;
    updateArtifact: (stepId: string, data: unknown[]) => void;
    executionProgress: ExecutionProgress | null;
    setExecutionProgress: React.Dispatch<React.SetStateAction<ExecutionProgress | null>>;

    reset: () => void;

    selectedResource: Resource | null;
    setSelectedResource: (resource: Resource | null) => void;

    chatInput: string;
    setChatInput: React.Dispatch<React.SetStateAction<string>>;
}

export type Resource = {
    type: "datasource" | "cluster";
    id: string;
    label: string;
    subType?: string;
    status?: "active" | "idle" | "error";
};

const AgentContext = createContext<AgentContextType | undefined>(undefined);

export function AgentProvider({ children }: { children: ReactNode }) {
    const [status, setStatus] = useState<AgentStatus>("IDLE");
    const [messages, setMessages] = useState<AgentMessage[]>([]);
    const [logs, setLogs] = useState<string[]>([]);
    const [plan, setPlan] = useState<ExecutionPlan | null>(null);
    const [executionProgress, setExecutionProgress] = useState<ExecutionProgress | null>(null);
    const [selectedResource, setSelectedResource] = useState<Resource | null>(null);
    const [chatInput, setChatInput] = useState<string>("");

    const addLog = (log: string) => {
        setLogs((prev) => [...prev, log]);
    };

    const addMessage = (msg: AgentMessage) => {
        setMessages((prev) => [...prev, msg]);
    }

    const updateStepStatus = (stepId: string, status: ExecutionPlan["steps"][0]["status"]) => {
        setPlan((prev) => {
            if (!prev) return null;
            return {
                ...prev,
                steps: prev.steps.map(step =>
                    step.id === stepId ? { ...step, status } : step
                )
            };
        });
    };

    const updateArtifact = (stepId: string, data: unknown[]) => {
        setPlan((prev) => {
            if (!prev) return null;
            return {
                ...prev,
                artifacts: {
                    ...(prev.artifacts || {}),
                    [stepId]: data
                }
            };
        });
    };

    const reset = () => {
        setStatus("IDLE");
        setLogs([]);
        setMessages([]);
        setPlan(null);
        setExecutionProgress(null);
        setSelectedResource(null);
    };

    return (
        <AgentContext.Provider
            value={{
                status, setStatus,
                messages, addMessage,
                logs, addLog,
                plan, setPlan,
                updateStepStatus,
                updateArtifact,
                executionProgress,
                setExecutionProgress,
                reset,
                selectedResource, setSelectedResource,
                chatInput, setChatInput
            }}
        >
            {children}
        </AgentContext.Provider>
    );
}

export function useAgent() {
    const context = useContext(AgentContext);
    if (context === undefined) {
        throw new Error("useAgent must be used within an AgentProvider");
    }
    return context;
}
