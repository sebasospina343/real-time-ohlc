import streamlit as st
import pandas as pd
from backend import get_features_from_fs
from plot import plot_data

st.write("Candle Graphs!")

online_or_offline = st.sidebar.selectbox("Select store", ["online", "offline"])

data = get_features_from_fs(online_or_offline)

# Debug information
st.write(f"Data shape: {data.shape}")
st.write(f"Columns: {list(data.columns)}")
if len(data) > 0:
    st.write("First few rows:")
    st.write(data.head())
    st.write("Data types:")
    st.write(data.dtypes)
    st.write("NaN counts:")
    st.write(data.isna().sum())

# st.table(data)

chart = plot_data(data)
st.bokeh_chart(chart)


