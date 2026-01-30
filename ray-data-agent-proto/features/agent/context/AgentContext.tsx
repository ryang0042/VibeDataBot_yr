"use client";

import React, { createContext, useContext, useState, ReactNode } from "react";

type AgentStep = "IDLE" | "THINKING" | "PLANNING" | "EXECUTING" | "DONE";

interface AgentContextType {
    status: AgentStep;
    setStatus: (status: AgentStep) => void;
    logs: string[];
    addLog: (log: string) => void;
    plan: any | null;
    setPlan: (plan: any) => void;
    reset: () => void;
    selectedResource: Resource | null;
    setSelectedResource: (resource: Resource | null) => void;
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
    const [status, setStatus] = useState<AgentStep>("IDLE");
    const [logs, setLogs] = useState<string[]>([]);
    const [plan, setPlan] = useState<any | null>(null);
    const [selectedResource, setSelectedResource] = useState<Resource | null>(null);

    const addLog = (log: string) => {
        setLogs((prev) => [...prev, log]);
    };

    const reset = () => {
        setStatus("IDLE");
        setLogs([]);
        setPlan(null);
        setSelectedResource(null);
    };

    return (
        <AgentContext.Provider value={{ status, setStatus, logs, addLog, plan, setPlan, reset, selectedResource, setSelectedResource }}>
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
