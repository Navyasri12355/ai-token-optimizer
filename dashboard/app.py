import streamlit as st
import requests
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import tiktoken
import os

# API URL — set via Streamlit secrets or environment variable
API_URL = st.secrets.get("API_URL", os.environ.get("API_URL", "http://127.0.0.1:8000"))

# ----------------------------
# Page Config
# ----------------------------
st.set_page_config(
    page_title="LLM Cost Optimizer",
    layout="wide",
    page_icon="💡"
)

# ----------------------------
# Custom CSS
# ----------------------------
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #1e2130, #252a3a);
        border-radius: 12px;
        padding: 16px 20px;
        border: 1px solid #2e3347;
        text-align: center;
    }
    .metric-label { color: #a0aec0; font-size: 13px; margin-bottom: 4px; }
    .metric-value { color: #e2e8f0; font-size: 28px; font-weight: 700; }
    .section-header {
        color: #7c83fd;
        font-size: 18px;
        font-weight: 600;
        margin: 24px 0 12px 0;
        border-bottom: 1px solid #2e3347;
        padding-bottom: 6px;
    }
</style>
""", unsafe_allow_html=True)

# ----------------------------
# Header
# ----------------------------
st.title("💡 LLM Token Cost Optimizer")
st.markdown("Optimize prompts and predict LLM usage cost in real-time")

# ----------------------------
# Input
# ----------------------------
prompt = st.text_area("📝 Enter your prompt", height=150, placeholder="Type your prompt here...")

# ----------------------------
# Analyze Button
# ----------------------------
if st.button("🚀 Analyze", use_container_width=True):

    if prompt.strip() == "":
        st.warning("Please enter a prompt.")
    else:
        with st.spinner("Analyzing prompt..."):
            try:
                response = requests.post(
                    f"{API_URL}/predict",
                    params={"prompt": prompt}
                )
                res = response.json()

                input_tokens      = res.get("input_tokens", 0)
                output_tokens     = res.get("output_tokens", 0)
                total_tokens      = res.get("total_tokens", 0)
                estimated_cost    = res.get("estimated_cost", 0)
                optimized_prompt  = res.get("optimized_prompt", "")
                token_savings_pct = res.get("token_savings_percent", 0)
                compression_pct   = res.get("compression_percent", 0)

                # compute original token count for comparison chart
                enc = tiktoken.encoding_for_model("gpt-3.5-turbo")
                original_tokens = len(enc.encode(prompt.strip()))
                optimized_tokens = len(enc.encode(optimized_prompt))

                # -------------------------------------------------------
                # Section 1: Key Metrics
                # -------------------------------------------------------
                st.markdown('<p class="section-header">📊 Token & Cost Summary</p>', unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Input Tokens",       input_tokens)
                c2.metric("Output Tokens",      output_tokens)
                c3.metric("Total Tokens",       total_tokens)
                c4.metric("Estimated Cost",     f"${estimated_cost:.6f}")

                st.markdown('<p class="section-header">📉 Savings Summary</p>', unsafe_allow_html=True)
                s1, s2 = st.columns(2)
                s1.metric("Token Savings",      f"{token_savings_pct:.1f}%")
                s2.metric("Char Compression",   f"{compression_pct:.1f}%")

                # -------------------------------------------------------
                # Section 2: Token Distribution — Donut Chart
                # -------------------------------------------------------
                st.markdown('<p class="section-header">🍩 Token Distribution</p>', unsafe_allow_html=True)
                col_donut, col_bar = st.columns(2)

                with col_donut:
                    fig1, ax1 = plt.subplots(figsize=(4.5, 4.5), facecolor="#0e1117")
                    ax1.set_facecolor("#0e1117")
                    sizes   = [input_tokens, output_tokens]
                    colors  = ["#7c83fd", "#f687b3"]
                    explode = (0.04, 0.04)
                    wedges, texts, autotexts = ax1.pie(
                        sizes,
                        explode=explode,
                        labels=["Input", "Output"],
                        colors=colors,
                        autopct="%1.1f%%",
                        startangle=140,
                        wedgeprops=dict(width=0.55, edgecolor="#0e1117", linewidth=2),
                        textprops={"color": "#e2e8f0", "fontsize": 12}
                    )
                    for at in autotexts:
                        at.set_color("#0e1117")
                        at.set_fontweight("bold")
                    ax1.set_title("Input vs Output Token Split", color="#a0aec0", fontsize=13, pad=10)
                    st.pyplot(fig1)
                    plt.close(fig1)

                # -------------------------------------------------------
                # Section 3: Original vs Optimized Tokens — Bar Chart
                # -------------------------------------------------------
                with col_bar:
                    fig2, ax2 = plt.subplots(figsize=(4.5, 4.5), facecolor="#0e1117")
                    ax2.set_facecolor("#1a1f2e")
                    labels = ["Original\nPrompt", "Optimized\nPrompt"]
                    values = [original_tokens, optimized_tokens]
                    bar_colors = ["#f687b3", "#68d391"]
                    bars = ax2.bar(labels, values, color=bar_colors, width=0.45,
                                   edgecolor="#0e1117", linewidth=1.5)
                    for bar, val in zip(bars, values):
                        ax2.text(bar.get_x() + bar.get_width() / 2,
                                 bar.get_height() + max(values) * 0.02,
                                 str(val), ha="center", va="bottom",
                                 color="#e2e8f0", fontweight="bold", fontsize=12)
                    ax2.set_ylabel("Tokens", color="#a0aec0", fontsize=11)
                    ax2.set_title("Original vs Optimized Tokens", color="#a0aec0", fontsize=13)
                    ax2.tick_params(colors="#a0aec0")
                    ax2.spines[:].set_color("#2e3347")
                    ax2.set_facecolor("#1a1f2e")
                    fig2.patch.set_facecolor("#0e1117")
                    st.pyplot(fig2)
                    plt.close(fig2)

                # -------------------------------------------------------
                # Section 4: Savings Gauge — Horizontal Progress Bars
                # -------------------------------------------------------
                st.markdown('<p class="section-header">📐 Savings Breakdown</p>', unsafe_allow_html=True)
                fig3, axes = plt.subplots(2, 1, figsize=(9, 3), facecolor="#0e1117")
                fig3.patch.set_facecolor("#0e1117")

                metrics = [
                    ("Token Savings %",      token_savings_pct, "#7c83fd"),
                    ("Char Compression %",   compression_pct,   "#f687b3"),
                ]
                for ax, (label, value, color) in zip(axes, metrics):
                    ax.set_facecolor("#1a1f2e")
                    ax.barh([0], [100], color="#2e3347", height=0.5)
                    ax.barh([0], [max(0, min(value, 100))], color=color, height=0.5)
                    ax.set_xlim(0, 100)
                    ax.set_yticks([])
                    ax.set_xticks(range(0, 101, 25))
                    ax.tick_params(colors="#a0aec0", labelsize=10)
                    ax.spines[:].set_color("#2e3347")
                    ax.set_xlabel("Percentage (%)", color="#a0aec0", fontsize=10)
                    ax.set_title(f"{label}: {value:.1f}%", color="#e2e8f0", fontsize=12, loc="left", pad=6)

                plt.tight_layout(pad=1.5)
                st.pyplot(fig3)
                plt.close(fig3)

                # -------------------------------------------------------
                # Section 5: Multi-turn Cost Projection — Line Chart
                # -------------------------------------------------------
                st.markdown('<p class="section-header">🔮 Multi-turn Cost Projection</p>', unsafe_allow_html=True)
                turns = st.slider("Number of conversation turns", 1, 20, 5)

                turn_range = np.arange(1, turns + 1)
                cost_per_turn     = estimated_cost * turn_range
                cost_optimized    = estimated_cost * (1 - token_savings_pct / 100) * turn_range

                fig4, ax4 = plt.subplots(figsize=(9, 3.5), facecolor="#0e1117")
                ax4.set_facecolor("#1a1f2e")
                ax4.plot(turn_range, cost_per_turn,  color="#f687b3", linewidth=2.5,
                         marker="o", markersize=5, label="Original Cost")
                ax4.plot(turn_range, cost_optimized, color="#68d391", linewidth=2.5,
                         marker="o", markersize=5, label="Optimized Cost", linestyle="--")
                ax4.fill_between(turn_range, cost_per_turn, cost_optimized,
                                 alpha=0.15, color="#7c83fd")
                ax4.set_xlabel("Conversation Turns", color="#a0aec0", fontsize=11)
                ax4.set_ylabel("Cumulative Cost ($)", color="#a0aec0", fontsize=11)
                ax4.set_title("Cumulative Cost Over Turns", color="#a0aec0", fontsize=13)
                ax4.tick_params(colors="#a0aec0")
                ax4.spines[:].set_color("#2e3347")
                ax4.legend(facecolor="#1a1f2e", labelcolor="#e2e8f0", fontsize=11)
                ax4.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:.5f}"))
                plt.tight_layout()
                st.pyplot(fig4)
                plt.close(fig4)

                total_savings = cost_per_turn[-1] - cost_optimized[-1]
                st.info(f"💰 Projected savings over **{turns} turns**: **${total_savings:.6f}**")

                # -------------------------------------------------------
                # Section 6: Feature Breakdown — Radar / Heatmap
                # -------------------------------------------------------
                st.markdown('<p class="section-header">🧩 Prompt Feature Profile</p>', unsafe_allow_html=True)

                words = prompt.strip().split()
                num_words   = len(words)
                avg_word_len = sum(len(w) for w in words) / max(num_words, 1)
                text_len    = len(prompt.strip())
                question_words = ["what", "why", "how", "explain", "describe"]
                question_flag  = int(any(q in prompt.lower() for q in question_words))

                feature_names = ["Text Length", "Word Count", "Avg Word Len", "Question Flag", "Total Tokens"]
                raw_values    = [text_len, num_words, avg_word_len, question_flag * 10, total_tokens / 10]
                max_vals      = [500, 100, 10, 10, 200]
                norm_values   = [min(v / m, 1.0) for v, m in zip(raw_values, max_vals)]

                fig5, ax5 = plt.subplots(figsize=(7, 3.5), facecolor="#0e1117")
                ax5.set_facecolor("#1a1f2e")
                x = np.arange(len(feature_names))
                bar_vals = [v * 100 for v in norm_values]
                gradient_colors = ["#7c83fd", "#a78bfa", "#f687b3", "#fcd34d", "#68d391"]
                bars5 = ax5.bar(x, bar_vals, color=gradient_colors,
                                edgecolor="#0e1117", linewidth=1.5, width=0.55)
                for bar, raw in zip(bars5, [text_len, num_words, f"{avg_word_len:.1f}", question_flag, total_tokens]):
                    ax5.text(bar.get_x() + bar.get_width() / 2,
                             bar.get_height() + 1.5,
                             str(raw), ha="center", va="bottom",
                             color="#e2e8f0", fontsize=10, fontweight="bold")
                ax5.set_xticks(x)
                ax5.set_xticklabels(feature_names, color="#a0aec0", fontsize=10)
                ax5.set_ylabel("Normalized Score (%)", color="#a0aec0", fontsize=11)
                ax5.set_title("Prompt Feature Profile", color="#a0aec0", fontsize=13)
                ax5.set_ylim(0, 120)
                ax5.tick_params(colors="#a0aec0")
                ax5.spines[:].set_color("#2e3347")
                plt.tight_layout()
                st.pyplot(fig5)
                plt.close(fig5)

                # -------------------------------------------------------
                # Section 7: Before vs After Prompt
                # -------------------------------------------------------
                st.markdown('<p class="section-header">✏️ Prompt Comparison</p>', unsafe_allow_html=True)
                col_orig, col_opt = st.columns(2)
                with col_orig:
                    st.write("**📝 Original Prompt**")
                    st.info(prompt)
                with col_opt:
                    st.write("**✂️ Optimized Prompt**")
                    st.success(optimized_prompt)

            except Exception as e:
                st.error("⚠️ API not running or error occurred")
                st.text(str(e))