import streamlit as st
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# MongoDB connection URI
uri = "mongodb+srv://db_user_laghchimoussama:olmongodb171@ol-cluster.3agvwhk.mongodb.net/NvidiaStockData?retryWrites=true&w=majority"

# Connect to MongoDB
client = MongoClient(uri)
db = client["NvidiaStockData"]

# Function to fetch data from a collection
def fetch_data(collection_name):
    data = list(db[collection_name].find())
    return pd.DataFrame(data)

# Streamlit app configuration
st.set_page_config(page_title="Nvidia Stock Dashboard", layout="wide", initial_sidebar_state="expanded")

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-title {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #76B900;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        margin-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-title">📊 Nvidia Stock Data Dashboard</h1>', unsafe_allow_html=True)

# Sidebar for collection selection
st.sidebar.title("⚙️ Configuration")
collections = ["stock_prices", "nvidia_reddit_posts", "nvidia_stocktwits_posts", "nvidia_yahoo_news"]
selected_collection = st.sidebar.selectbox("📂 Sélectionner une collection:", collections)

# Fetch and display data
data = fetch_data(selected_collection)

# Download data as CSV in sidebar
with st.sidebar:
    st.markdown("---")
    st.subheader("💾 Export des données")
    csv = data.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Télécharger CSV",
        data=csv,
        file_name=f"{selected_collection}.csv",
        mime="text/csv",
        use_container_width=True
    )

# 1. Courbe du prix de clôture (Close) dans le temps
def plot_closing_price(df):
    st.subheader("1. 📈 Courbe du prix de clôture")
    if "Date" in df.columns and "Close" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)
        df = df.sort_values("Date")
        fig = px.line(df, x="Date", y="Close", title="Évolution du prix de clôture")
        fig.update_traces(line_color='#1f77b4')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Les colonnes 'Date' et 'Close' sont nécessaires pour cette visualisation.")

# 2. Graphique en barres du volume échangé
def plot_daily_volume(df):
    st.subheader("2. 📊 Volume échangé quotidien")
    if "Date" in df.columns and "Volume" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)
        df = df.sort_values("Date")
        fig = px.bar(df, x="Date", y="Volume", title="Volume total échangé par jour")
        fig.update_traces(marker_color='#2ca02c')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Les colonnes 'Date' et 'Volume' sont nécessaires pour cette visualisation.")

# 3. Courbes des prix hauts et bas (High vs Low)
def plot_high_low_prices(df):
    st.subheader("3. 🔺🔻 Prix hauts et bas (High vs Low)")
    if "Date" in df.columns and "High" in df.columns and "Low" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)
        df = df.sort_values("Date")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["Date"], y=df["High"], mode='lines', name='High', line=dict(color='red')))
        fig.add_trace(go.Scatter(x=df["Date"], y=df["Low"], mode='lines', name='Low', line=dict(color='orange')))
        fig.update_layout(title="Prix maximum et minimum quotidiens", xaxis_title="Date", yaxis_title="Prix")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Les colonnes 'Date', 'High' et 'Low' sont nécessaires pour cette visualisation.")

# 4. Bougies japonaises (Candlestick chart)
def plot_candlestick(df):
    st.subheader("4. 🕯️ Bougies japonaises (Candlestick)")
    if all(col in df.columns for col in ["Date", "Open", "High", "Low", "Close"]):
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)
        df = df.sort_values("Date")
        fig = go.Figure(data=[go.Candlestick(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"]
        )])
        fig.update_layout(title="Chandelier japonais (OHLC)", xaxis_title="Date", yaxis_title="Prix")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Les colonnes 'Date', 'Open', 'High', 'Low' et 'Close' sont nécessaires.")

# 5. Moyenne mobile du prix de clôture
def plot_moving_average(df):
    st.subheader("5. 📊 Moyenne mobile du prix de clôture (7 jours)")
    if "Date" in df.columns and "Close" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)
        df = df.sort_values("Date")
        df["MA_7"] = df["Close"].rolling(window=7).mean()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["Date"], y=df["Close"], mode='lines', name='Close', line=dict(color='blue')))
        fig.add_trace(go.Scatter(x=df["Date"], y=df["MA_7"], mode='lines', name='MA 7 jours', line=dict(color='red', dash='dash')))
        fig.update_layout(title="Prix de clôture avec moyenne mobile 7 jours", xaxis_title="Date", yaxis_title="Prix")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Les colonnes 'Date' et 'Close' sont nécessaires pour cette visualisation.")

# 6. Histogramme des rendements journaliers
def plot_daily_returns(df):
    st.subheader("6. 📉 Histogramme des rendements journaliers")
    if "Close" in df.columns:
        df = df.sort_values("Date")
        df["Daily_Return"] = df["Close"].pct_change() * 100
        fig = px.histogram(df.dropna(), x="Daily_Return", nbins=50, title="Distribution des rendements journaliers (%)")
        fig.update_traces(marker_color='purple')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("La colonne 'Close' est nécessaire pour cette visualisation.")

# 7. Heatmap des volumes par jour de semaine
def plot_volume_heatmap(df):
    st.subheader("7. 🔥 Heatmap des volumes par jour de semaine")
    if "Date" in df.columns and "Volume" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)
        df["Day_of_Week"] = df["Date"].dt.day_name()
        df["Week"] = df["Date"].dt.isocalendar().week
        pivot_table = df.pivot_table(values="Volume", index="Day_of_Week", aggfunc="mean")
        
        fig, ax = plt.subplots(figsize=(10, 4))
        sns.heatmap(pivot_table.T, annot=True, fmt=".0f", cmap="YlOrRd", ax=ax)
        plt.title("Volume moyen par jour de la semaine")
        st.pyplot(fig)
    else:
        st.write("Les colonnes 'Date' et 'Volume' sont nécessaires pour cette visualisation.")

# 8. Graphique comparatif High vs Close
def plot_high_vs_close(df):
    st.subheader("8. 🔵 Corrélation High vs Close")
    if "High" in df.columns and "Close" in df.columns:
        fig = px.scatter(df, x="High", y="Close", title="Comparaison High vs Close", trendline="ols")
        fig.update_traces(marker=dict(color='teal', size=5))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Les colonnes 'High' et 'Close' sont nécessaires pour cette visualisation.")

# 9. Indice de volatilité (High – Low)
def plot_volatility_index(df):
    st.subheader("9. 📊 Indice de volatilité (High - Low)")
    if "Date" in df.columns and "High" in df.columns and "Low" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)
        df = df.sort_values("Date")
        df["Volatility"] = df["High"] - df["Low"]
        fig = px.line(df, x="Date", y="Volatility", title="Volatilité quotidienne (High - Low)")
        fig.update_traces(line_color='darkred')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Les colonnes 'Date', 'High' et 'Low' sont nécessaires pour cette visualisation.")

# 10. Tableau de bord synthétique (KPI Cards)
def display_kpi_cards(df):
    st.subheader("10. 📌 Indicateurs clés (KPI)")
    if "Close" in df.columns and "Volume" in df.columns and "High" in df.columns and "Low" in df.columns:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_close = df["Close"].mean()
            st.metric("Prix de clôture moyen", f"${avg_close:.2f}")
        
        with col2:
            avg_volume = df["Volume"].mean()
            st.metric("Volume moyen", f"{avg_volume:,.0f}")
        
        with col3:
            max_high = df["High"].max()
            st.metric("Plus haut historique", f"${max_high:.2f}")
        
        with col4:
            min_low = df["Low"].min()
            st.metric("Plus bas historique", f"${min_low:.2f}")
    else:
        st.write("Les colonnes nécessaires pour les KPI ne sont pas disponibles.")

# Fixing Arrow serialization issues and timezone-aware datetime

def clean_dataframe(df):
    # Convert ObjectId to string
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    # Ensure all datetime columns are timezone-naive
    for col in df.select_dtypes(include=["datetime", "object"]):
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None)
        except Exception:
            pass

    return df

# Apply cleaning before displaying or processing data
data = clean_dataframe(data)

# Add visualizations to the dashboard
if selected_collection == "stock_prices":
    # Display KPI cards first
    display_kpi_cards(data)
    
    st.markdown("---")
    
    # Create tabs for organized dashboard
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Tendances & Prix", "📊 Volume & Volatilité", "🔬 Analyse Technique", "📉 Statistiques"])
    
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            plot_closing_price(data)
        with col2:
            plot_candlestick(data)
        
        plot_moving_average(data)
        plot_high_low_prices(data)
    
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            plot_daily_volume(data)
        with col2:
            plot_volatility_index(data)
        
        plot_volume_heatmap(data)
    
    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            plot_daily_returns(data)
        with col2:
            plot_high_vs_close(data)
    
    with tab4:
        st.subheader("📋 Statistiques descriptives")
        st.dataframe(data.describe(), use_container_width=True)
        
        with st.expander("🔍 Voir les données brutes (100 premières lignes)"):
            st.dataframe(data.head(100), use_container_width=True)

else:
    # For other collections, display basic info
    st.info(f"📊 Collection sélectionnée: **{selected_collection}**")
    st.metric("Nombre d'enregistrements", len(data))
    
    with st.expander("🔍 Voir les données"):
        st.dataframe(data, use_container_width=True)
    
    with st.expander("📊 Statistiques de base"):
        st.write(data.describe())