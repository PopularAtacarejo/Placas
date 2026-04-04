@echo off
setlocal ENABLEDELAYEDEXPANSION
chcp 65001 >nul

cd /d "%~dp0"

set "ARQ_CDR=Validade.cdr"
if not exist "%ARQ_CDR%" (
  echo Arquivo base nao encontrado: "%ARQ_CDR%"
  pause
  exit /b 1
)

where python >nul 2>nul
if errorlevel 1 (
  echo Python nao encontrado no PATH.
  echo Instale o Python 3 e marque a opcao "Add Python to PATH".
  pause
  exit /b 1
)

echo =================================================
echo [APP] AUTOMACAO DE OFERTAS DE VALIDADE
echo =================================================
echo.
echo [1] ENTRADA
echo    Use Excel, PDF, TXT ou texto bruto colado.
echo.
echo [2] LOGIN E ANALISE WEB
echo    Primeiro o sistema abre a tela de login no navegador.
echo    Depois abra a central web, escolha o arquivo
echo    ou cole o texto e clique em "Analisar".
echo.
echo [3] PRODUCAO DAS PLACAS
echo    Selecione as placas e imprima sem salvar no CDR.
echo    O processo avanca automaticamente.
echo.
echo [4] IA LOCAL (OPCIONAL)
echo    Com Ollama instalado, o sistema tambem corrige
echo    descricoes ambiguas automaticamente.
echo.
echo [5] LOG NO GITHUB (OPCIONAL)
echo    Se GITHUB_PLACAS_TOKEN estiver definida no Windows,
echo    cada placa concluida sera registrada com data e hora.
echo.

python "%~dp0atualizar_por_planilha.py" --arquivo-cdr "%ARQ_CDR%" --sem-confirmacao-impressao --usar-ia-local
set "RET=%ERRORLEVEL%"

echo.
if "%RET%"=="0" (
  echo Automacao concluida com sucesso.
) else (
  echo Automacao falhou. Codigo: %RET%
)

pause
exit /b %RET%

