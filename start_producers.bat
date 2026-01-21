@echo off
echo ===================================================
echo 🚀 LANCEMENT DU PIPELINE DE DONNEES (PRODUCERS)
echo ===================================================

:: 1. Lancement du Producer Bourse (Yahoo Finance)
echo Démarrage de YFinance...
start "Stream Bourse (NVDA)" cmd /k "venv\Scripts\activate.bat && python src/producers/producer_yfinance.py"

:: 2. Lancement du Producer Reddit
echo Démarrage de Reddit...
start "Stream Reddit" cmd /k "venv\Scripts\activate.bat && python src/producers/producer_reddit.py"

:: 3. Lancement du Producer StockTwits
echo Démarrage de StockTwits...
start "Stream StockTwits" cmd /k "venv\Scripts\activate.bat && python src/producers/producer_stocktwits.py"
:: 4. Lancement du Producer Yahoo News
echo Démarrage de Yahoo News...
start "Stream Yahoo News" cmd /k "venv\Scripts\activate.bat && python src/producers/producer_yahoonews.py"

echo.
echo ✅ Tous les scripts ont été lancés dans des fenêtres séparées.
echo Ne fermez pas ces fenêtres tant que vous voulez envoyer des données !
pause