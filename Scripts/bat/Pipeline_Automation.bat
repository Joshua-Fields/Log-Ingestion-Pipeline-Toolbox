@echo off
rem ------------------------------------------------------------------------------
rem Usage:
rem   Reduce_RFIQD_size.bat "C:\path\to\file1.rfiqd" "C:\path\to\file2.rfiqd" ...
rem        [--outdir C:\output\dir] [--header-size 8192] ...
rem
rem Description:
rem   This batch file calls the updated Python code (`Reduce_RFIQD_size.py`)
rem   which supports multiple .rfiqd inputs. All command-line arguments are
rem   forwarded directly to the Python script.
rem
rem Example:
rem   Reduce_RFIQD_size.bat "C:\Data\A.rfiqd" "C:\Data\B.rfiqd" --outdir "C:\ReducedOut"
rem ------------------------------------------------------------------------------

rem 1) Figure out the directory of this .bat file (so we can find the .py file)
set "script_dir=%~dp0"

rem 2) Change directory to that folder, so Python can import / see the script
pushd "%script_dir%"

rem 3) Forward all arguments (%*) to the Python script.
rem    Example if the user typed:
rem       Reduce_RFIQD_size.bat "C:\Data\A.rfiqd" "C:\Data\B.rfiqd" --outdir ...
rem    Then we pass exactly those arguments to Python.
python pipelineAutomation.py %*

rem 4) Go back to where we started
popd
