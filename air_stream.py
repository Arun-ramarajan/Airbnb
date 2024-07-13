import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
pd.set_option('display.max_columns', None)

df = pd.read_csv("C:/Users/LENOVO/Desktop/airbnb/Airbnb_preprocessed.csv")

with st.sidebar:    
    select= option_menu("Main Menu", ["Home", "Data Exploration", "About"])