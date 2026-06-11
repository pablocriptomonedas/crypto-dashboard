@echo off
echo =============================================
echo   Crypto Expert Dashboard - Iniciando...
echo =============================================
echo.
echo Instalando dependencias...
pip install -r requirements.txt --quiet
echo.
echo Iniciando servidor...
echo Abre tu navegador en: http://localhost:8000
echo.
echo Para cerrar el dashboard pulsa Ctrl+C
echo.
python main.py
pause
