
# save as app.py and run: streamlit run app.py
import time
import streamlit as st

st.set_page_config(page_title="Mini Calculator", layout="centered")

st.title("Lever Edge: Time-Saving Calculator")
st.markdown("Enter your inputs and click **Compute**. A spinner will show while processing.")

# Inputs (use integers for counts)
homes = st.number_input("Number of Homes", value=10, step=1, min_value=0, format="%d")
workers = st.number_input("Number of Workers", value=5, step=1, min_value=0, format="%d")
outlets = st.slider("Number of Outlets per Home", min_value=1, max_value=48, value=12, step=1)

# Action
if st.button("Compute"):
    # Basic validation
    if homes == 0 or workers == 0:
        st.error("Please enter at least 1 home and 1 worker.")
    else:
        with st.spinner("Calculating..."):
            time.sleep(0.8)  # simulate work

            # Original math kept (sum/product/scaled) but with clearer variable names
            sum_hw = float(homes + workers)
            product_hw = float(homes * workers)
            scaled_sum = sum_hw * float(outlets)

        st.success("Done!")
        st.subheader("Results")
        st.write(f"Sum (Homes + Workers): **{sum_hw:.2f}**")
        st.write(f"Product (Homes × Workers): **{product_hw:.2f}**")
        st.write(f"Scaled sum ((Homes + Workers) × Outlets per Home): **{scaled_sum:.2f}**")
