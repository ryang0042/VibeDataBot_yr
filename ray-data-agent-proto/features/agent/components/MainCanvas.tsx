import React, { useEffect, useRef, useState } from "react";
import { useAgent, Resource } from "@/features/agent/context/AgentContext";
import { PipelineVisualizer } from "@/features/pipeline/components/PipelineVisualizer";
import { DataFrame } from "@/features/data-view/components/DataFrame";
import { ResourceDetailView } from "@/features/resources/components/ResourceDetailView";
import { motion, AnimatePresence } from "framer-motion";

const MOCK_SOURCE_DATA = [
    { id: "101", timestamp: "2024-01-20T10:00:00Z", message: "User login from 192.168.1.1", email: "alice@example.com" },
    { id: "102", timestamp: "2024-01-20T10:05:00Z", message: "Purchase completed", email: "bob@gmail.com" },
    { id: "103", timestamp: "2024-01-20T10:12:00Z", message: "Failed login attempt", email: "charlie@corp.net" },
];

const MOCK_PII_DATA = [
    { id: "101", timestamp: "2024-01-20T10:00:00Z", message: "User login from [IP]", email: "[EMAIL_REDACTED]" },
    { id: "102", timestamp: "2024-01-20T10:05:00Z", message: "Purchase completed", email: "[EMAIL_REDACTED]" },
    { id: "103", timestamp: "2024-01-20T10:12:00Z", message: "Failed login attempt", email: "[EMAIL_REDACTED]" },
];

export function MainCanvas() {
    const { status, logs, selectedResource } = useAgent();
    // ... (rest of component logic same as before)
    const bottomRef = useRef<HTMLDivElement>(null);
    const [activePreview, setActivePreview] = useState<string | null>(null);

    useEffect(() => {
        if (bottomRef.current) {
            bottomRef.current.scrollIntoView({ behavior: "smooth" });
        }
    }, [logs, status, activePreview]);

    // If a resource is selected in the Sidebar, show its details instead of the Chat/Pipeline
    if (selectedResource) {
        return <ResourceDetailView resource={selectedResource} />;
    }

    return (
        <div className="flex-1 w-full bg-background/50 relative overflow-hidden flex flex-col">
            {/* Background Grid Pattern */}
            <div
                className="absolute inset-0 opacity-[0.03] pointer-events-none"
                style={{
                    backgroundImage: `radial-gradient(circle at 1px 1px, currentColor 1px, transparent 0)`,
                    backgroundSize: "24px 24px",
                }}
            />

            <div className="flex-1 overflow-y-auto p-4 sm:p-8 pb-32 scroll-smooth">
                <div className="max-w-4xl mx-auto flex flex-col gap-6">
                    {/* Welcome Message - Only show when IDLE */}
                    {status === "IDLE" && (
                        <div className="flex flex-col gap-2 p-8 rounded-xl border border-border/50 bg-card/30 backdrop-blur-sm text-center items-center justify-center min-h-[400px]">
                            <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-primary to-purple-400">
                                Data Engineering, Reimagined.
                            </h1>
                            <p className="text-muted-foreground max-w-lg">
                                Describe your data task, and VibeDataBot will orchestrate the Ray cluster to execute it at scale.
                            </p>
                        </div>
                    )}

                    {/* Planning Phase */}
                    {(status === "PLANNING" || status === "EXECUTING" || status === "DONE") && (
                        <motion.div
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            className="flex flex-col gap-4"
                        >
                            <div className="flex items-center gap-2 text-sm text-muted-foreground">
                                <span className="w-2 h-2 rounded-full bg-primary animate-pulse" />
                                Generated Execution Plan
                            </div>
                            <PipelineVisualizer onNodeClick={setActivePreview} />
                        </motion.div>
                    )}

                    {/* Data Preview Modal Area */}
                    <AnimatePresence>
                        {activePreview === "source" && (
                            <motion.div
                                initial={{ opacity: 0, height: 0 }}
                                animate={{ opacity: 1, height: "auto" }}
                                exit={{ opacity: 0, height: 0 }}
                                className="overflow-hidden"
                            >
                                <DataFrame
                                    title="Source Data Preview (S3)"
                                    data={MOCK_SOURCE_DATA}
                                    columns={["id", "timestamp", "message", "email"]}
                                    className="border-primary/20"
                                />
                            </motion.div>
                        )}
                        {activePreview === "pii" && (
                            <motion.div
                                initial={{ opacity: 0, height: 0 }}
                                animate={{ opacity: 1, height: "auto" }}
                                exit={{ opacity: 0, height: 0 }}
                                className="overflow-hidden"
                            >
                                <DataFrame
                                    title="PII Scan Results (Presidio)"
                                    data={MOCK_PII_DATA}
                                    columns={["id", "timestamp", "message", "email"]}
                                    highlightColumns={["email", "message"]}
                                    className="border-red-500/20 shadow-red-500/5"
                                />
                            </motion.div>
                        )}
                    </AnimatePresence>

                    {/* Logs / Thinking Stream */}
                    <div className="flex flex-col gap-2 font-mono text-sm mt-4">
                        {logs.map((log, i) => (
                            <motion.div
                                key={i}
                                initial={{ opacity: 0, x: -10 }}
                                animate={{ opacity: 1, x: 0 }}
                                transition={{ delay: i * 0.05 }}
                                className="p-3 rounded border border-border/50 bg-black/20 text-muted-foreground whitespace-pre-wrap"
                            >
                                {log}
                            </motion.div>
                        ))}
                        {(status === "THINKING" || status === "EXECUTING") && (
                            <div className="flex items-center gap-2 text-primary/50 p-2 animate-pulse">
                                Processing...
                            </div>
                        )}
                    </div>

                    <div ref={bottomRef} />
                </div>
            </div>
        </div>
    );
}
