@echo OFF
set OLDPP=%PYTHONPATH%
set PYTHONPATH=.
..\..\venv\ciw\Scripts\python.exe %1 %2 %3 %4 %5 %6 %7 %8 %9
set PYTHONPATH=%OLDPP%
