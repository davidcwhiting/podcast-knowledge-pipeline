"""Streamlit chat interface for the podcast knowledge pipeline."""

import os

import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://api:8000")

st.set_page_config(page_title="Podcast Knowledge Chat", page_icon="🎙️", layout="wide")
st.title("🎙️ Podcast Knowledge Chat")
st.caption("Ask questions about content from Joe Rogan, Huberman Lab, and Lex Fridman")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message.get("sources"):
            with st.expander("Sources"):
                for s in message["sources"]:
                    st.markdown(f"**{s['episode']}** ({s['channel']}) — similarity: {s['similarity']}")
                    st.caption(s["excerpt"])

# Chat input
if prompt := st.chat_input("Ask about podcast content..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get response from API
    with st.chat_message("assistant"):
        with st.spinner("Searching transcripts..."):
            try:
                response = requests.post(
                    f"{API_URL}/chat",
                    json={"question": prompt},
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                st.markdown(data["answer"])

                if data.get("sources"):
                    with st.expander("Sources"):
                        for s in data["sources"]:
                            st.markdown(f"**{s['episode']}** ({s['channel']}) — similarity: {s['similarity']}")
                            st.caption(s["excerpt"])

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": data["answer"],
                    "sources": data.get("sources", []),
                })
            except Exception as e:
                error_msg = f"Error: {e}"
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})

# Sidebar with quick stats
with st.sidebar:
    st.header("Quick Stats")
    try:
        episodes = requests.get(f"{API_URL}/episodes?limit=100", timeout=5).json()
        guests = requests.get(f"{API_URL}/guests?limit=100", timeout=5).json()
        st.metric("Episodes", len(episodes))
        st.metric("Guests", len(guests))
    except Exception:
        st.caption("Stats unavailable")

    st.divider()
    st.header("Example Questions")
    st.caption("• What does Jensen Huang think about AI scaling?")
    st.caption("• What topics do guests discuss most?")
    st.caption("• What books have been recommended?")
    st.caption("• Compare opinions on leadership")
