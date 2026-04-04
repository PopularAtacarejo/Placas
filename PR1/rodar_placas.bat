@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "PY_CMD="
where py >nul 2>&1
if not errorlevel 1 set "PY_CMD=py -3"
if not defined PY_CMD (
  where python >nul 2>&1
  if not errorlevel 1 set "PY_CMD=python"
)
if not defined PY_CMD (
  echo [ERRO] Python nao encontrado no computador.
  echo [ERRO] Instale o Python 3 e tente novamente.
  pause
  endlocal & exit /b 9009
)

set "OUTPUT_DIR=%SCRIPT_DIR%saida"
set "PDF_SUBDIR=pdf"
set "PROFILE=A4_FOLHA_COMPLETA"
set "OLLAMA_MODEL=qwen3:1.7b"
set "TEMPLATE_CDR=%SCRIPT_DIR%Modelos De Placa\A4.cdr"

echo [FORMATO] Defina A4/A5/A6 na tela "Configuracao das placas".
echo [PADRAO] Inicio em A4.
echo.

if not exist "%TEMPLATE_CDR%" (
  if exist "%SCRIPT_DIR%A4.cdr" (
    set "TEMPLATE_CDR=%SCRIPT_DIR%A4.cdr"
  ) else (
    echo [TPL] Template padrao nao encontrado: "%TEMPLATE_CDR%"
    echo [TPL] Sera usado fallback "%SCRIPT_DIR%Placa.cdr".
    set "TEMPLATE_CDR=%SCRIPT_DIR%Placa.cdr"
  )
)

if "%~1"=="" (
  %PY_CMD% "%SCRIPT_DIR%agente_placas_corel.py" ^
    --select-input ^
    --template-cdr "%TEMPLATE_CDR%" ^
    --output-dir "%OUTPUT_DIR%" ^
    --pdf-subdir "%PDF_SUBDIR%" ^
    --name-col "DESCRICAO" ^
    --price-col "OFERTA R$" ^
    --price-prefix "" ^
    --use-ollama-cleanup ^
    --ollama-model "%OLLAMA_MODEL%" ^
    --font-name "AhkioW00-Bold" ^
    --profile "%PROFILE%" ^
    --stop-hotkey "f7" ^
    --close-corel
) else (
  set "INPUT_PDF=%~1"
  %PY_CMD% "%SCRIPT_DIR%agente_placas_corel.py" ^
    --input-pdf "%INPUT_PDF%" ^
    --template-cdr "%TEMPLATE_CDR%" ^
    --output-dir "%OUTPUT_DIR%" ^
    --pdf-subdir "%PDF_SUBDIR%" ^
    --name-col "DESCRICAO" ^
    --price-col "OFERTA R$" ^
    --price-prefix "" ^
    --use-ollama-cleanup ^
    --ollama-model "%OLLAMA_MODEL%" ^
    --font-name "AhkioW00-Bold" ^
    --profile "%PROFILE%" ^
    --stop-hotkey "f7" ^
    --close-corel
)

set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERRO] Execucao finalizada com codigo %EXIT_CODE%.
  echo [ERRO] Verifique o log em "%SCRIPT_DIR%erro_execucao_placas.log"
  pause
)

endlocal & exit /b %EXIT_CODE%
