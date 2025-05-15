@echo off
cd /d %~dp0

set /p MSG="Digite a mensagem do commit: "
if "%MSG%"=="" (
    set MSG=Atualização do site
)

echo.
echo Adicionando arquivos...
git add .

echo Criando commit com mensagem: %MSG%
git commit -m "%MSG%"

echo Enviando para o GitHub...
git push origin main

echo.
echo Site atualizado com sucesso!
pause
