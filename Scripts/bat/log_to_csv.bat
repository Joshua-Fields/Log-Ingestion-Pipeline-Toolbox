@echo off
rem Get the directory of this batch script (BatchScripts folder)
set "batch_dir=%~dp0"

rem Move to the parent of the BatchScripts folder (delimiter/)
pushd "%batch_dir%..\"

rem Now run the Python script inside Scripts
"C:\Users\Joshua.Fields\AppData\Local\Programs\Python\Python311\python.exe" log_to_csv.py %*

rem Return to the original directory
popd
pause
