TLVArchive downloader.

It will download files from https://archive-binyan.tel-aviv.gov.il

Input - chromdriver path
      - csv files which contains Tat rove, Gush and Chelka list, as Downloaded from the municipal GIS.
      (must have the following fields: "ktatrova", "ms_gush", "ms_chelka")
      - Output dir

Requirements and notes-
     - Runs on Windows.
     - python 3.8
     - chromdriver which align to chrom, 
     For example, version 130 can be downloaded from -
     https://storage.googleapis.com/chrome-for-testing-public/130.0.6723.69/win32/chromedriver-win32.zip
     - Python packages selenium and pandas

RUNNING EXAMPLE -
    # Setting python virtual env
    pip install virtualenv
    virtualenv env
    call env/Scripts/activate.bat

    # Installing packages
    pip install selenium
    pip install pandas

    # Run the app
    python main.py  --chrome chromedriver_win32/chromedriver.exe
                    --cores 8
                    --input input/GushChelka_All.csv
                    --output output
