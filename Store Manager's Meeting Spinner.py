
# lever_edge_calculator.py
# Streamlit app to replicate Leviton Lever Edge labor/time savings calculator
# Author: M365 Copilot (for John Hritz)
# Date: 2026-01-07

import time
import math
import streamlit as st

# ---------- Page config ----------
st.set_page_config(
    page_title="Lever Edge Labor & Time Savings Calculator",
    page_icon="⚡",
    layout="centered"
)

# ---------- Styles ----------
st.markdown("""
<style>
/* Make the metric cards a bit tighter */
.block-container {padding-top: 1.2rem; padding-bottom: 1.2rem;}
div[data-testid="stMetricValue"] {font-weight: 700;}
.small-note {font-size: 0.85rem; color: #6b7280;}
.hr {border-top: 1px solid #e5e7eb; margin: 1rem 0;}
</style>
""", unsafe_allow_html=True)

# ---------- Header ----------
st.title("Lever Edge – Labor & Time Savings Calculator")
st.caption("Compare Traditional wiring vs Lever Edge devices using the inputs below.")

# ---------- Inputs ----------
with st.form("inputs_form", clear_on_submit=False):
    col1, col2 = st.columns(2)
    with col1:
        devices = st.number_input(
            "1) Number of devices to install",
            min_value=1, step=1, value=50
        )
        traditional_rate_per_hour = st.number_input(
            "2) Traditional installs per hour",
            min_value=0.1, step=0.1, value=10.0,
            help="How many traditional (screw-terminal) devices you typically install per hour."
        )
    with col2:
        labor_rate = st.number_input(
            "3) Labor rate ($/hour)",
            min_value=0.0, step=1.0, value=75.0
        )
        speed_factor = st.slider(
            "Lever Edge speed factor (× faster than traditional)",
            min_value=1.0, max_value=4.0, value=3.5, step=0.1,
            help="The Leviton page notes 'up to 3.5× faster'. Adjust if you have local validation data."
        )

    spin_duration = st.slider(
        "Spinner delay (seconds)",
        min_value=0.5, max_value=4.0, value=2.2, step=0.1,
        help="Artificial delay to let the loader spin a little longer."
    )

    calculate = st.form_submit_button("Calculate Savings ⚙️")

# ---------- Calculation ----------
def compute(devices, trad_rate, labor_rate, speed_factor):
    # Traditional
    trad_hours = devices / trad_rate
    trad_cost = trad_hours * labor_rate

    # Lever Edge
    lever_rate = trad_rate * speed_factor
    lever_hours = devices / lever_rate
    lever_cost = lever_hours * labor_rate

    # Savings
    time_saved = trad_hours - lever_hours
    dollars_saved = trad_cost - lever_cost
    pct_time_reduction = (time_saved / trad_hours * 100.0) if trad_hours > 0 else 0.0
    pct_cost_reduction = (dollars_saved / trad_cost * 100.0) if trad_cost > 0 else 0.0

    # Round for display (not truncate), but keep underlying math precise
    def r2(x): return math.floor(x * 100 + 0.5) / 100.0

    return {
        "trad_hours": trad_hours,
        "trad_cost": trad_cost,
        "lever_hours": lever_hours,
        "lever_cost": lever_cost,
        "time_saved": time_saved,
        "dollars_saved": dollars_saved,
        "pct_time_reduction": pct_time_reduction,
        "pct_cost_reduction": pct_cost_reduction,
        # rounded
        "r_trad_hours": r2(trad_hours),
        "r_trad_cost": r2(trad_cost),
        "r_lever_hours": r2(lever_hours),
        "r_lever_cost": r2(lever_cost),
        "r_time_saved": r2(time_saved),
        "r_dollars_saved": r2(dollars_saved),
        "r_pct_time_reduction": r2(pct_time_reduction),
        "r_pct_cost_reduction": r2(pct_cost_reduction),
    }

if calculate:
    with st.spinner("Crunching the numbers… Push, click, and you're done!"):
        time.sleep(spin_duration)  # artificial delay so it "spins a little long"
        results = compute(devices, traditional_rate_per_hour, labor_rate, speed_factor)

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)
    st.subheader("Results")

    # Two-column comparison
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Traditional")
        st.metric("Install Time (hours)", f"{results['r_trad_hours']:,}")
        st.metric("Labor Cost ($)", f"{results['r_trad_cost']:,}")
    with c2:
        st.markdown("### Lever Edge")
        st.metric("Install Time (hours)", f"{results['r_lever_hours']:,}")
        st.metric("Labor Cost ($)", f"{results['r_lever_cost']:,}")

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    # Savings summary
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Time Saved (hours)", f"{results['r_time_saved']:,}")
    s2.metric("Labor Dollars Saved ($)", f"{results['r_dollars_saved']:,}")
    s3.metric("% Time Reduction", f"{results['r_pct_time_reduction']:,}%")
    s4.metric("% Cost Reduction", f"{results['r_pct_cost_reduction']:,}%")

    st.markdown(
        "<p class='small-note'>Assumptions: Lever Edge is treated as up to 3.5× faster than traditional screw-terminal wiring; "
        "adjust the speed factor as needed based on your own jobsite data.</p>",
        unsafe_allow_html=True
    )

# ---------- Footnote / Source ----------
st.markdown(
    "<p class='small-note'>Source: Leviton Lever Edge page (inputs and 'up to 3.5× faster').</p>",
    unsafe_allow_html=True
)
