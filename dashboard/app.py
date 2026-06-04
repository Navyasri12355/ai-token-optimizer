import os
import streamlit as st
import requests
import matplotlib.pyplot as plt

# ----------------------------
# Page Config
# ----------------------------
st.set_page_config(
    page_title="LLM Cost Optimizer",
    layout="wide"
)

st.title("💡 LLM Token Cost Optimizer")
st.markdown("Optimize prompts and predict LLM usage cost in real-time")

# Read API URL from env (set to Azure Container Apps URL after deployment)
# Default falls back to localhost for local dev
API_URL = os.environ.get("API_URL", "http://127.0.0.1:8000")

# ----------------------------
# Input
# ----------------------------
prompt = st.text_area("📝 Enter your prompt", height=150)

# ----------------------------
# Analyze Button
# ----------------------------
if st.button("🚀 Analyze"):

    if prompt.strip() == "":
        st.warning("Please enter a prompt.")
    else:
        try:
            response = requests.post(
                f"{API_URL}/predict",
                params={"prompt": prompt}
            )

            res = response.json()

            # ----------------------------
            # Token Breakdown
            # ----------------------------
            st.subheader("📊 Token Breakdown")

            col1, col2, col3 = st.columns(3)

            col1.metric("Input Tokens", res.get("input_tokens", 0))
            col2.metric("Output Tokens", res.get("output_tokens", 0))
            col3.metric("Total Tokens", res.get("total_tokens", 0))

            # ----------------------------
            # Cost Analysis
            # ----------------------------
            st.subheader("💰 Cost Analysis")
            st.metric("Estimated Cost ($)", f"${res.get('estimated_cost', 0):.6f}")

            # ----------------------------
            # Before vs After Prompt
            # ----------------------------
            col1, col2 = st.columns(2)

            with col1:
                st.write("### 📝 Original Prompt")
                st.info(prompt)

            with col2:
                st.write("### ✂️ Optimized Prompt")
                st.success(res.get("optimized_prompt", ""))

            st.subheader("📉 Savings Analysis")
            st.metric("Token Savings (%)", res["token_savings_percent"])
            st.metric("Compression (%)", res["compression_percent"])

            # ----------------------------
            # Token Graph
            # ----------------------------
            st.subheader("📈 Token Distribution")

            input_tokens = res.get("input_tokens", 0)
            output_tokens = res.get("output_tokens", 0)

            labels = ["Input Tokens", "Output Tokens"]
            values = [input_tokens, output_tokens]

            fig, ax = plt.subplots()
            ax.bar(labels, values)
            ax.set_ylabel("Token Count")
            ax.set_title("Input vs Output Tokens")

            st.pyplot(fig)

            # ----------------------------
            # Future Cost Simulation
            # ----------------------------
            st.subheader("🔮 Multi-turn Cost Projection")

            turns = st.slider("Number of conversation turns", 1, 10, 3)

            future_cost = res.get("estimated_cost", 0) * turns

            st.write(f"Estimated cost for {turns} turns: ${future_cost:.6f}")

        except Exception as e:
            st.error("⚠️ API not running or error occurred")
            st.text(str(e))