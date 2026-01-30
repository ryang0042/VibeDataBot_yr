"use client";

import React, { useState } from "react";
import { Send, Sparkles, Loader2 } from "lucide-react";
import { useAgent } from "@/features/agent/context/AgentContext";
import { cn } from "@/lib/utils";

export function ChatInterface() {
    const [input, setInput] = useState("");
    const { status, setStatus, addLog } = useAgent();

    const handleSend = async () => {
        if (!input.trim() || status !== "IDLE") return;

        // Simulation Flow
        setStatus("THINKING");
        addLog(`> ${input}`);
        setInput("");

        // Simulate Agent Thinking
        setTimeout(() => {
            addLog("Analyzing request...");
        }, 500);

        setTimeout(() => {
            addLog("Found data source: s3://customer-logs/2024/*");
        }, 1200);

        setTimeout(() => {
            addLog("Generating Ray Data pipeline for PII detection...");
        }, 2000);

        setTimeout(() => {
            setStatus("PLANNING");
            addLog("Plan generated. Ready to execute.");
        }, 3000);
    };

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    return (
        <div className="w-full bg-card/80 backdrop-blur-md border border-primary/20 shadow-2xl shadow-primary/5 rounded-xl overflow-hidden transition-all focus-within:ring-1 focus-within:ring-primary/50 focus-within:border-primary/50">
            <div className="flex flex-col p-1">
                <textarea
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Ask VibeDataBot to load data, scan for PII, or transform specific columns..."
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
                        {status === "THINKING" ? <Loader2 size={16} className="animate-spin" /> : <Send size={16} />}
                    </button>
                </div>
            </div>
        </div>
    );
}
