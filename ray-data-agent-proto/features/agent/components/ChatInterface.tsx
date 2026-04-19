"use client";

import React from "react";
import { Send, Sparkles, Loader2 } from "lucide-react";
import { useAgent } from "@/features/agent/context/AgentContext";
import { MockPlanGenerator } from "@/features/agent/logic/PlanGenerator";
import { ExecutionEngine } from "@/features/agent/logic/ExecutionEngine";
import { Play } from "lucide-react";
import { cn } from "@/lib/utils";

export function ChatInterface() {
    const { 
        status, setStatus, addMessage, setPlan, plan, 
        updateStepStatus, addLog, updateArtifact, setExecutionProgress,
        chatInput: input, setChatInput: setInput 
    } = useAgent();

    const handleSend = async () => {
        if (!input.trim() || status !== "IDLE") return;

        const userMsg = input;
        setInput("");
        setExecutionProgress(null);

        // 1. Add User Message
        addMessage({
            id: Date.now().toString(),
            sender: "user",
            content: userMsg,
            timestamp: Date.now()
        });

        // 2. Set Thinking
        setStatus("THINKING");

        try {
            // 3. Generate Plan (Mock LLM)
            const plan = await MockPlanGenerator.generate(userMsg);

            setPlan(plan);
            setStatus("PLANNING");

            addMessage({
                id: (Date.now() + 1).toString(),
                sender: "agent",
                content: `I've generated a plan to "${userMsg}". Check the pipeline view above. Click 'Execute' when ready.`,
                timestamp: Date.now(),
                relatedPlanId: plan.id
            });

        } catch (error) {
            console.error(error);
            setStatus("ERROR");
            addMessage({
                id: Date.now().toString(),
                sender: "agent",
                content: "Sorry, I failed to generate a plan. Please try again.",
                timestamp: Date.now()
            });
        }
    };

    const handleExecute = async () => {
        if (!plan) return;
        setStatus("EXECUTING");

        try {
            await ExecutionEngine.executePlan(plan, {
                onStepUpdate: (stepId, status) => {
                    // Map 'active' | 'completed' | 'failed' to Step status
                    const mappedStatus = status === "active" ? "running" : status;
                    updateStepStatus(stepId, mappedStatus);
                },
                onLog: (log) => addLog(log),
                onArtifact: (stepId, data) => updateArtifact(stepId, data),
                onProgress: (progress) => setExecutionProgress(progress),
            });

            setStatus("DONE");
            addMessage({
                id: Date.now().toString(),
                sender: "agent",
                content: "Plan execution completed successfully.",
                timestamp: Date.now()
            });
            setTimeout(() => {
                setStatus("IDLE");
                setExecutionProgress(null);
            }, 3000); // Reset after 3s

        } catch (e) {
            console.error(e);
            setStatus("ERROR");
        }
    };

    const handleKeyDown = (e: React.KeyboardEvent) => {
        // 阻止在输入法组合态（如打拼音选词敲击的空格/回车）时意外触发提交
        if (e.nativeEvent.isComposing) return;

        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    if (status === "PLANNING") {
        return (
            <div className="w-full bg-card/80 backdrop-blur-md border border-primary/20 shadow-2xl shadow-primary/5 rounded-xl overflow-hidden p-4 flex items-center justify-between">
                <div className="text-sm text-muted-foreground">
                    Plan is ready. Verify steps above.
                </div>
                <button
                    onClick={handleExecute}
                    className="flex items-center gap-2 px-4 py-2 bg-green-500 hover:bg-green-600 text-white rounded-lg transition-all font-medium shadow-lg shadow-green-500/20"
                >
                    <Play size={16} fill="currentColor" />
                    Execute Plan
                </button>
            </div>
        );
    }

    return (
        <div className="w-full bg-card/80 backdrop-blur-md border border-primary/20 shadow-2xl shadow-primary/5 rounded-xl overflow-hidden transition-all focus-within:ring-1 focus-within:ring-primary/50 focus-within:border-primary/50">
            <div className="flex flex-col p-1">
                <textarea
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Ask VibeDataBot to extract a PDF, run 15D quality scoring, or build a cleaned training corpus..."
                    className="w-full bg-transparent border-none focus:ring-0 text-foreground placeholder:text-muted-foreground/50 resize-none min-h-[3rem] px-4 py-3 text-[15px] outline-none"
                    rows={1}
                    disabled={status !== "IDLE"}
                />
                <div className="flex items-center justify-between px-2 pb-2">
                    <div className="flex items-center gap-1">
                        <button className="flex items-center gap-1.5 px-2 py-1 rounded text-xs text-muted-foreground hover:bg-muted/50 hover:text-foreground transition-colors">
                            <Sparkles size={12} />
                            <span>Suggestions</span>
                        </button>
                    </div>
                    <button
                        onClick={handleSend}
                        disabled={!input.trim() || status !== "IDLE"}
                        className={cn(
                            "h-8 w-8 flex items-center justify-center rounded-lg transition-all shadow-sm",
                            input.trim() && status === "IDLE"
                                ? "bg-primary text-primary-foreground hover:bg-primary/90"
                                : "bg-muted text-muted-foreground cursor-not-allowed"
                        )}
                    >
                        {status === "THINKING" || status === "EXECUTING" ? <Loader2 size={16} className="animate-spin" /> : <Send size={16} />}
                    </button>
                </div>
            </div>
        </div>
    );
}
