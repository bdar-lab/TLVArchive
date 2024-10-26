"""
Welcome to TLVArchive downloader.
It will download files from https://archive-binyan.tel-aviv.gov.il
Input - chromdriver path
      - csv files which contains Tat rove, Gush and Chelka list, as Downloaded from the municipal GIS.
      (must have the following fields: "ktatrova", "ms_gush", "ms_chelka")
      - Output dir

Requirements and notes-
     - Runs on Windows.
     - python 3.8
     - chromdriver, can be downloaded from -
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
"""

import os
import time
import traceback
import re
import glob
import pandas
import argparse
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from queue import Queue
from threading import Thread, Lock

# Chrome driver path
CHROME_DRIVER = "chromedriver_win32/chromedriver.exe"

# Number of cores to run
CORES = 8

# Input files
INPUT_FILES = ["input/GushChelka_All.csv"]

# Waiting time in seconds
WAIT_TIME = 120

# Run chrome headless
HEADLESS = False

# Flag for waiting to all files
WAIT_ALL = True

# Website to search
HOMEPAGE = 'https://archive-binyan.tel-aviv.gov.il'

# Input mandatory fields
KTATROVA_FIELD = "ktatrova"
GUSH_FIELD = "ms_gush"
CHELKA_FIELD = "ms_chelka"

# Status for "status" field
COMPLETED = "completed"
MISMATCH = "mismatch"
EMPTY = ""

# No results field
NO_RESULTS = "no_results"

# Results
RESULTS_DIR = "output"

CURR_TIME = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
FILE_PREFIX = "log__"
LOG_FILE = f"{FILE_PREFIX}{CURR_TIME}.txt"

MERGED_REPORT = f"{FILE_PREFIX}report_{CURR_TIME}.txt"
MERGED_CSV_FILE = f"{FILE_PREFIX}documents_{CURR_TIME}.csv"
DOC_COLUMNS = [KTATROVA_FIELD,
               GUSH_FIELD,
               CHELKA_FIELD,
               "multiple_gush_chelka",
               "address",
               "tik_id",
               "page_number",
               "row_number",
               "date",
               "type",
               "request",
               "permit",
               "size",
               "document"]

TIK_COLUMNS = [KTATROVA_FIELD,
               GUSH_FIELD,
               CHELKA_FIELD,
               "tik_id",
               "status",  # 'completed' or 'mismatch'
               NO_RESULTS,  # 'no_results' or empty
               "docs_in_csv",
               "docs_in_directory",
               "docs_in_web",
               "docs_in_csv_not_in_dir",
               "docs_in_dir_not_in_csv"]

# Index in data tables
DATE = 0
TYPE = 1
REQUEST = 3
PERMIT = 4
SIZE = 5
PDFBTN = 7

data_lock = Lock()


def merge_input_csvs():
    input_csvs = []
    ktatrovas = set()
    input_dir = os.path.dirname(INPUT_FILES[0])
    for input_csv_path in INPUT_FILES:
        input_csv = pandas.read_csv(input_csv_path, converters={i: str for i in range(0, 30)})
        input_csvs.append(input_csv)

    combined_input_csv = pandas.concat(input_csvs)

    for field in combined_input_csv:
        if field == KTATROVA_FIELD:
            ktatrovas.update([x for x in combined_input_csv[field]])
            break
    ktatrovas = sorted(ktatrovas)
    combined_input_csv_file = f"Ktatrova_{'_'.join(ktatrovas)}.csv"
    combined_input_csv_path = os.path.join(input_dir, combined_input_csv_file)
    combined_input_csv.to_csv(combined_input_csv_path)
    return combined_input_csv_path, ktatrovas


def read_status_csvs(input_csv_path, ktatrovas):
    status_csvs = {}
    for ktatrova in ktatrovas:
        # Print reading input csv
        to_log(ktatrova, f"Tatrova {ktatrova} Created merged csv input file ({input_csv_path})...")

        # Get status csv if exists
        status_csv_file = f"{FILE_PREFIX}status_TatRova{ktatrova}.csv"
        status_csv_path = os.path.join(RESULTS_DIR, ktatrova, status_csv_file)
        if os.path.exists(status_csv_path):
            # csv exists, read it
            to_log(ktatrova, f"Tatrova {ktatrova} Reading status csv ({status_csv_path})")
            status_csv = CsvTikHolder()
            status_csv.read_csv(status_csv_path)
            status_csvs[ktatrova] = status_csv

    return status_csvs


def activate_threads(input_csv, status_csvs):
    # Create a queue to communicate with the worker threads
    queue = Queue()

    # Create worker threads
    for x in range(CORES):
        worker = DownloadWorker(queue)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()

    # Put all gush-chelka in queue
    for i in range(len(input_csv.ktatrova)):
        ktatrova = input_csv.ktatrova[i]
        gush = input_csv.ms_gush[i]
        chelka = input_csv.ms_chelka[i]

        if ktatrova not in status_csvs:
            status_csvs[ktatrova] = CsvTikHolder()
        gush_chelka = GushChelka(ktatrova, gush, chelka, status_csvs[ktatrova])
        gush_chelka.queue = queue
        gush_chelka.update_fields()
        queue.put(gush_chelka)

    queue.join()


def wait_for_doc(ktatrova, doc_path, size="0MB"):
    timer = 0
    r = re.search(r"(\d+.?\d+)MB", size)
    doubler = float(r.group(1)) if r else 1
    if doubler > 1:
        doubler = doubler / 10 + 1
    while timer < WAIT_TIME * doubler and not os.path.exists(doc_path):
        time.sleep(10)
        message = f"Waiting {timer} out of {WAIT_TIME * doubler} sec " \
                  f"for document ({doc_path})..."
        to_log(ktatrova, message)
        timer += 10


def get_web_count(driver):
    web_count_element = driver.find_elements(By.XPATH, "//h3/span")
    if web_count_element and "נמצאו" in web_count_element[0].text:
        return int(web_count_element[0].text.split(' ')[1])
    return 0


def get_multiple_gush_chelka(driver):
    mult_elem = driver.find_elements(By.XPATH, "//div[@class='blocks']/ul/li")
    if mult_elem:
        mult = [x.text for x in mult_elem]
        if len(mult) > 1:
            return ", ".join(mult)
    return ""


def get_address(driver):
    mult_elem = driver.find_elements(By.XPATH, "//div[@class='addresses']/ul/li")
    if mult_elem:
        mult = [x.text for x in mult_elem]
        if len(mult) > 1:
            return ", ".join(mult)
    return ""


def check_security(driver):
    proceed_btn = driver.find_elements(By.XPATH, "//button[@id='proceed-button']")
    if proceed_btn:
        proceed_btn[0].click()


def merge_csvs(ktatrovas):
    for ktatrova in ktatrovas:
        ktatrova_dir = os.path.join(RESULTS_DIR, ktatrova)
        all_csvs = glob.glob(f"{ktatrova_dir}/**/*.csv", recursive=True)
        all_csvs_filtered = [x for x in all_csvs if "status" not in x and "documents" not in x]
        csvs_count = len(all_csvs_filtered)

        to_log(ktatrova, f"Finished collecting all data for ktatrova {ktatrova}, "
                         f"Merging {csvs_count} csv files...")

        # Read all csvs
        if all_csvs_filtered:
            tiks_csv = []
            count = 1
            for f in all_csvs_filtered:
                to_log(ktatrova, f"csv ({count} out of {csvs_count}): {f}")
                res_csv = CsvDocHolder()
                res_csv.read_csv(f)
                tiks_csv.append(res_csv)
                count += 1

            # Merge all csvs
            combined_csv = tiks_csv[0]
            for res_csv in tiks_csv[1:]:
                combined_csv.extend(res_csv)

            # Export to merged csv
            merged_csv_path = os.path.join(RESULTS_DIR, ktatrova, MERGED_CSV_FILE)
            combined_csv.to_csv(merged_csv_path)
        else:
            to_log(ktatrova, f"No csv files found.")


# Must be like TIK_COLUMNS
def create_status_record(ktatrova="",
                         gush="",
                         chelka="",
                         tik_id="",
                         status="",
                         no_results="",
                         docs_in_csv=0,
                         docs_in_directory=0,
                         docs_in_web=0,
                         docs_in_csv_not_in_dir=0,
                         docs_in_dir_not_in_csv=0):
    return [ktatrova,
            gush,
            chelka,
            tik_id,
            status,
            no_results,
            docs_in_csv,
            docs_in_directory,
            docs_in_web,
            docs_in_csv_not_in_dir,
            docs_in_dir_not_in_csv]


def verify_and_generate_reports(input_csv, status_csvs, ktatrovas):
    # Create a queue to communicate with the worker threads
    queue = Queue()

    # Create worker threads
    for x in range(len(ktatrovas)):
        worker = ReportWorker(queue)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()

    # Put all reporters in queue
    reporters = []
    for ktatrova in ktatrovas:
        status_csv = status_csvs[ktatrova]
        reporter = Reporter(ktatrova, input_csv, status_csv)
        reporters.append(reporter)

        queue.put(reporter)

    queue.join()
    return reporters


def merge_reports(input_csv, reporters):
    if not reporters:
        for ktatrova in set(input_csv.ktatrova):
            to_log(ktatrova, "No reports found!")

    merged_reporter = Reporter(None, None, None)
    merged_reporter.reporters = reporters

    for reporter in reporters:
        # Merge reports
        merged_reporter.gush_chelka_received += reporter.gush_chelka_received
        merged_reporter.tiks_received += reporter.tiks_received
        merged_reporter.completed.extend(reporter.completed)
        merged_reporter.mismatches.extend(reporter.mismatches)
        merged_reporter.waiting.extend(reporter.waiting)
        merged_reporter.docs_in_csv_count += reporter.docs_in_csv_count
        merged_reporter.docs_in_dir_count += reporter.docs_in_dir_count
        merged_reporter.docs_in_web_count += reporter.docs_in_web_count

        # get mismatches between csv and dir
        merged_reporter.in_csv_not_in_dir += reporter.in_csv_not_in_dir
        merged_reporter.in_dir_not_in_csv += reporter.in_dir_not_in_csv

        # Finish!
        to_log(reporter.ktatrova,
               f"Tatrova {reporter.ktatrova} report can be found under {reporter.path}")

    merged_reporter.write_report(is_merged_report=True)


def to_log(ktatrova, message):
    if not ktatrova:
        print(message)
        return

    log_dir = os.path.join(RESULTS_DIR, ktatrova)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_path = os.path.join(log_dir, LOG_FILE)
    with open(log_path, "a", encoding="utf-8") as log:
        print(message)
        log.write(f"{message}\n")
        log.flush()


class DownloadWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue
            gush_chelka = self.queue.get()

            try:
                gush_chelka.download_docs_and_get_data()

            except (ValueError, Exception):
                message = f"---------UNEXPECTED ERROR---------\n" \
                          f"{gush_chelka.prefix}\n" \
                          f"{traceback.format_exc()}\n" \
                          f"-----------------------------------"

                to_log(gush_chelka.ktatrova, message)

                gush_chelka.res_csv.to_csv(gush_chelka.res_csv_path)
                gush_chelka.update_status_csv()

                if gush_chelka.docs_in_csv < gush_chelka.docs_in_web:
                    gush_chelka.start_idx = gush_chelka.current_row
                    self.queue.put(gush_chelka)

                if not self.queue.empty():
                    continue

            finally:
                self.queue.task_done()


class ReportWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue
            reporter = self.queue.get()

            try:
                reporter.verify_ktatrova()
                reporter.write_report()

            except (ValueError, Exception):
                message = f"---------UNEXPECTED ERROR---------\n" \
                          f"{traceback.format_exc()}\n" \
                          f"-----------------------------------"

                to_log(reporter.ktatrova, message)

            finally:
                self.queue.task_done()


# function to take care of downloading file
def enable_download_headless(driver, download_dir):
    driver.command_executor._commands["send_command"] = \
        ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior',
              'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    driver.execute("send_command", params)


class GushChelka:

    def __init__(self, ktatrova, gush, chelka, status_csv):

        self.ktatrova = ktatrova
        self.gush = gush
        self.chelka = chelka

        # For handling web
        self.driver = None
        self.url = HOMEPAGE

        # For holding status
        self.status_csv = status_csv
        self.status_csv_dir = ""
        self.status_file = ""
        self.status_path = ""
        self.status_index = -1
        self.status_key = ""

        # For holding no results status field
        self.no_results = ""

        # For Tik jobs
        self.tik_id = ""

        # For Gush-Chelka job
        self.tik_ids = []
        self.tik_indexes = []

        # documents results
        self.res_csv = CsvDocHolder()
        self.res_dir = ""
        self.res_csv_file = ""
        self.res_csv_path = ""

        # For prints and reports
        self.prefix = ""

        # For res csv
        self.multiple_gush_chelka = ""
        self.address = ""

        # Document counts
        self.docs_in_web = 0
        self.docs_in_csv = 0
        self.docs_in_directory = 0

        # For ERROR Exception messages
        self.current_page = 0
        self.current_row = 0
        self.start_idx = 0

        # For gush-chelka threads to add tiks
        self.queue = None

    def update_fields(self, only_prefix=False):
        if not only_prefix:
            self.status_csv_dir = os.path.join(RESULTS_DIR, self.ktatrova)
            self.status_file = f"{FILE_PREFIX}status_TatRova{self.ktatrova}.csv"
            self.status_path = os.path.join(self.status_csv_dir, self.status_file)
            self.res_dir = os.path.join(RESULTS_DIR, self.ktatrova, self.tik_id)

            if self.tik_id:
                self.res_csv_file = f"{FILE_PREFIX}TatRova{self.ktatrova}_{self.tik_id}.csv"
                self.res_csv_path = os.path.join(self.res_dir, self.res_csv_file)
                self.url = f"{HOMEPAGE}/pages/results.aspx?owstikid={self.tik_id}"
                self.status_key = f"{self.ktatrova}_{self.gush}_{self.chelka}_{self.tik_id}"
                if self.status_key in self.status_csv.key_to_index:
                    self.status_index = self.status_csv.key_to_index[self.status_key]

        self.prefix = f"(Ktatrova {self.ktatrova}, " \
                      f"Gush {self.gush}, " \
                      f"Chelka {self.chelka}, " \
                      f"ID {self.tik_id}, " \
                      f"Page {self.current_page}, " \
                      f"Row {self.current_row})"

    # Target for Gush-Chelka /Tik workers threads
    # Gush-Chelka - when arrived from main from input file
    #   if not in status - add it
    #   if in status - Handle first Tik and add other to queue
    # Tik when arrived from threads
    def download_docs_and_get_data(self):

        # Check for repeated Tiks
        if self.start_idx > 0:
            try:
                self.launch_and_accept_policy()
                self.handle_table()
                self.res_csv.to_csv(self.res_csv_path)
                self.update_status_csv()

            finally:
                # Close homepage
                if self.driver:
                    self.driver.quit()

        # Find Gush-Chelka/Tik in status (might be multiple indexes for both)
        self.get_indexes_from_status()

        # Update tik_ids with all uncompleted tiks
        tik_ids = set([self.status_csv.tik_id[i] for i in self.tik_indexes
                       if self.status_csv.status[i] != COMPLETED])

        is_completed = True if self.tik_indexes and not tik_ids else False

        if is_completed:
            to_log(self.ktatrova, f"{self.prefix} Gush-Chelka was already completed.")
            return

        if tik_ids:
            self.tik_ids = list(tik_ids)
            self.tik_id = self.tik_ids[0]
            self.update_fields()

        if not os.path.exists(self.res_dir):
            os.makedirs(self.res_dir)
            to_log(self.ktatrova, f"{self.prefix} Creating directory {self.res_dir} succeeded.")

        if self.tik_id and self.is_active():
            return

        # Open page with selenium webdriver
        try:
            self.launch_and_accept_policy()

            # No tik id. go get it
            if not self.tik_id:

                # Get tiks for given gush chelka
                self.navigate_to_gush_chelka()
                self.get_tik_ids()

                # Add tiks to status
                # Mark all gush-chelka with no results as completed
                self.add_status_records()

                # No results
                if len(self.tik_ids) == 0:
                    return

            # Put in queue second tik and up
            if self.tik_ids:
                self.put_tiks_in_queue()

            if self.tik_id:
                self.search_no_results_message(False)
                if self.no_results == NO_RESULTS:
                    with data_lock:
                        self.status_csv.status[self.status_index] = COMPLETED
                        self.status_csv.no_results[self.status_index] = NO_RESULTS
                else:
                    self.current_page = 1
                    self.handle_table()
                    self.res_csv.to_csv(self.res_csv_path)
                self.update_status_csv()

        finally:
            # Close homepage
            if self.driver:
                self.driver.quit()

    def is_active(self):
        with data_lock:
            active_threads = self.status_csv.tik_ids_queued
            if self.tik_id not in active_threads:
                # Activate
                active_threads.add(self.tik_id)
                return False

        return True

    # Add tiks of gush-chelka to queue starting with the second one
    def put_tiks_in_queue(self):
        start = 1 if self.tik_id else 0
        for tik_id in self.tik_ids[start:]:
            tik = GushChelka(self.ktatrova, self.gush,
                             self.chelka, self.status_csv)
            tik.tik_id = tik_id
            tik.update_fields()
            self.queue.put(tik)

    def get_indexes_from_status(self):

        # Tik
        if self.tik_id:
            key = f"{self.ktatrova}_" \
                  f"{self.gush}_" \
                  f"{self.chelka}_" \
                  f"{self.tik_id}"
            if key in self.status_csv.key_to_index:
                self.tik_indexes.append(self.status_csv.key_to_index[key])
                return

        # Gush-chelka
        for i in range(len(self.status_csv.ktatrova)):
            if self.ktatrova == self.status_csv.ktatrova[i] and \
                    self.gush == self.status_csv.ms_gush[i] and \
                    self.chelka == self.status_csv.ms_chelka[i]:
                self.tik_indexes.append(i)

    # Only for Tik job with tik_id
    def is_tik_completed(self):
        if self.tik_id and \
                self.status_key in self.status_csv.key_to_index:
            i = self.status_csv.key_to_index[self.status_key]
            return self.status_csv.status[i] == COMPLETED

        return False

    def get_tik_ids(self):
        self.tik_ids = []
        multiple_tiks = \
            self.driver.find_elements(By.XPATH, "//a[@countlinkstblfolderlink][@href]")
        if multiple_tiks:
            self.tik_ids = [x.text for x in multiple_tiks]

        # Single tik or no results
        else:
            tik_id_elem = self.driver.find_elements(By.XPATH, "//li[@class='bread_last']")
            if tik_id_elem:
                tik_id = tik_id_elem[0].text
                if "תיק מספר" in tik_id:
                    record_id = re.search(r"(\d+)", tik_id).group(1)
                    self.tik_ids = [record_id]

        # No Tiks found, find no-results message to verify
        if not self.tik_ids:
            self.search_no_results_message(True)

    def search_no_results_message(self, must_found):
        no_results_elem = self.driver.find_elements(By.XPATH, "//div/div/p/strong")

        no_results_message = f"{self.prefix} Gush-Chelka search gave no results."
        # No results for gush chelka
        if no_results_elem and "לא נמצאו תיקי בניין" in no_results_elem[0].text:
            to_log(self.ktatrova, no_results_message)
            self.no_results = NO_RESULTS

        # Couldn't find nor tik id nor no-results message
        elif must_found:
            to_log(self.ktatrova, f"{self.prefix} "
                                  f"POTENTIAL ERROR! {no_results_message} "
                                  f"without proper message!")

    def launch_and_accept_policy(self):
        # instantiate a chrome options object, so you can set the size and headless preference
        # some of these chrome options might be unnecessary, but I just used a boilerplate
        # change the <path_to_download_default_directory> to whatever your default download folder is located
        options = Options()
        if HEADLESS:
            options.add_argument("--headless")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-notifications")
        options.add_argument('--no-sandbox')
        options.add_argument('--verbose')
        options.add_experimental_option("prefs", {
            "download.default_directory": self.res_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing_for_trusted_sources_enabled": False,
            "safebrowsing.enabled": False
        })
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        service = Service(executable_path=CHROME_DRIVER)
        # Initialize driver object and wet the path to chrome driver
        self.driver = webdriver.Chrome(options=options, service=service)

        # Setting up headless download
        if HEADLESS:
            enable_download_headless(self.driver, self.res_dir)
        url = self.url
        if self.start_idx > 0:
            url = f"{url}&PageIndex={self.current_page - 1}"
        self.driver.get(url)
        message = f"{self.prefix} Launching {url}"
        to_log(self.ktatrova, message)
        continue_button = self.driver.find_elements(By.XPATH, "//a[@class='arc-button-big']")
        if continue_button:
            continue_button[0].click()
        else:
            message = f"{self.prefix} POTENTIAL ERROR! Could not find accept policy continue button."
            to_log(self.ktatrova, message)

    def navigate_to_gush_chelka(self):

        search_methods = self.driver.find_elements(By.XPATH,
                                                   "//select[@class='search-methods']/option[text()='גוש חלקה']")
        if search_methods:
            search_methods[0].click()
        else:
            message = f"{self.prefix} ERROR! Could not find search method button."
            to_log(self.ktatrova, message)
            return

        search_block = self.driver.find_elements(By.XPATH, "//input[@id='search_blocks_input']")
        if search_block:
            if len(self.chelka) <= 3:
                search_block[0].send_keys(self.gush)
        else:
            message = f"{self.prefix} Could not find input box for gush..."
            to_log(self.ktatrova, message)
            return

        search_parcels = self.driver.find_elements(By.XPATH, "//input[@id='search_parcels_input']")
        if search_parcels and search_block:

            # Handle chelka with more than 3 digits
            if len(self.chelka) > 3:
                # Send fake value to gush to select and cut to keyboard
                search_block[0].send_keys(self.chelka)
                search_block[0].send_keys(Keys.CONTROL + "a")
                search_block[0].send_keys(Keys.CONTROL + "x")

                # Gush is empty... send correct value to gush
                search_block[0].send_keys(self.gush)

                # Paste value to chelka
                search_parcels[0].send_keys(Keys.CONTROL + "v")
            else:
                search_parcels[0].send_keys(self.chelka)
        else:
            message = f"{self.prefix} Could not find input box for chelka..."
            to_log(self.ktatrova, message)
            return

        search_btn = self.driver.find_elements(By.XPATH, "//a[@class='benefits btn_general']")
        if search_btn:
            search_btn[0].click()
        else:
            message = f"{self.prefix} Could not find continue button for gush chelka search..."
            to_log(self.ktatrova, message)
            return

    def add_status_records(self):
        records = []

        # No results record
        if not self.tik_ids:
            records.append(create_status_record(ktatrova=self.ktatrova,
                                                gush=self.gush,
                                                chelka=self.chelka,
                                                status=COMPLETED,
                                                no_results=NO_RESULTS))

        else:
            for tik_id in self.tik_ids:
                records.append(create_status_record(ktatrova=self.ktatrova,
                                                    gush=self.gush,
                                                    chelka=self.chelka,
                                                    tik_id=tik_id))

        # Reading and writing to shared status_csvs
        # must be done with lock
        with data_lock:

            # Update and save status
            self.status_csv.extend(records)
            self.status_csv.to_csv(self.status_path)

    def handle_table(self):

        # Get number of docs and multiple gush chelka field from web
        if self.current_page == 1 and self.start_idx == 0:
            self.docs_in_web = get_web_count(self.driver)
            with data_lock:
                self.status_csv.docs_in_web[self.status_index] = str(self.docs_in_web)

                # Save status
                self.status_csv.to_csv(self.status_path)

            self.multiple_gush_chelka = get_multiple_gush_chelka(self.driver)
            self.address = get_address(self.driver)

        trs = self.driver.find_elements(By.XPATH, "//tr[contains(@class,'row') "
                                                  "and contains(@class,'draggable')]")

        # Iterate over table rows (trs)
        self.current_row = self.start_idx
        for tr in trs[self.start_idx:]:
            self.current_row += 1
            self.update_fields(True)
            tds = tr.find_elements(By.XPATH, ".//td")

            # Iterate over row cell (tds)
            # Download file and get data
            self.handle_table_row(tds)

            # Handle error,
            back_btn = self.driver.find_elements(By.XPATH, "//a[@href='javascript: history.go(-1)']")
            if back_btn:
                back_btn[0].click()
                time.sleep(5)
                self.start_idx = self.current_row
                self.handle_table()
                return

        # Stop endless recursion
        if self.docs_in_csv > self.docs_in_web:
            message = f"{self.prefix} ERROR: " \
                      f"csv count exceeded web count {self.docs_in_web}"
            to_log(self.ktatrova, message)
            return

        # Handle next page recursively
        next_page_btn = self.driver.find_elements(By.XPATH, "//li[@aria-label='לעמוד הבא']")
        if next_page_btn:
            next_page_btn[0].find_elements(By.XPATH, ".//a")[0].click()
            self.current_page += 1
            self.start_idx = 0
            self.update_fields()
            # Save status
            self.status_csv.to_csv(self.status_path)
            self.handle_table()

    def handle_table_row(self, tds):
        date, doc_type, request, permit, size = "", "", "", "", ""

        for i, td in enumerate(tds):
            if i == DATE:
                date = td.text
            elif i == TYPE:
                doc_type = td.text
            elif i == REQUEST:
                request = td.text
            elif i == PERMIT:
                permit = td.text
            elif i == SIZE:
                size = td.text
            elif i == PDFBTN:
                doc_file = td.get_attribute("documentid")
                doc_path = os.path.join(self.res_dir, doc_file)

                record = self.create_record(date=date,
                                            doc_type=doc_type,
                                            request=request,
                                            permit=permit,
                                            size=size,
                                            doc_path=doc_path)

                # Update csv
                self.res_csv.add(record)
                self.docs_in_csv += 1

                # Update status with lock
                with data_lock:
                    self.status_csv.docs_in_csv[self.status_index] = str(self.docs_in_csv)

                # Download if not exists
                if os.path.exists(doc_path):
                    message = f"{self.prefix} Document already exists " \
                              f"({self.docs_in_csv} out of {self.docs_in_web}) " \
                              f"size {size}, {doc_file}."

                    to_log(self.ktatrova, message)
                    return

                # Download document
                td.click()

                message = f"{self.prefix} " \
                          f"Downloading Document " \
                          f"({self.docs_in_csv} out of {self.docs_in_web}) " \
                          f"size {size}, {doc_file}."

                to_log(self.ktatrova, message)

                # Wait for all documents
                if WAIT_ALL:
                    wait_for_doc(self.ktatrova, doc_path, size)

                # Wait for last document only
                elif self.docs_in_csv == self.docs_in_web:
                    wait_for_doc(self.ktatrova, doc_path)

                return

    def update_status_csv(self):

        dir_docs = glob.glob(f"{self.res_dir}/*.*")
        filtered_dir_docs = [x for x in dir_docs if FILE_PREFIX not in x]

        docs_in_dir_not_in_csv = [x for x in filtered_dir_docs
                                  if x not in self.res_csv.document]

        docs_in_csv_not_in_dir = [x for x in self.res_csv.document
                                  if x not in filtered_dir_docs]

        # Update status csv for all tiks with same tik_id
        duplicated_tiks = [i for i, val in enumerate(self.status_csv.tik_id) if val == self.tik_id]
        for i in duplicated_tiks:
            if self.status_csv.docs_in_web[i] == "0" and self.docs_in_web > 0:
                self.status_csv.docs_in_web[i] = str(self.docs_in_web)
            self.status_csv.docs_in_csv[i] = str(self.docs_in_csv)

            self.status_csv.docs_in_directory[i] = \
                str(len(filtered_dir_docs))
            self.status_csv.docs_in_dir_not_in_csv[i] = \
                str(len(docs_in_dir_not_in_csv))
            self.status_csv.docs_in_csv_not_in_dir[i] = \
                str(len(docs_in_csv_not_in_dir))

            if 0 < self.docs_in_csv == self.docs_in_web <= len(filtered_dir_docs) \
                    and not docs_in_csv_not_in_dir:
                self.status_csv.status[i] = COMPLETED
            elif self.status_csv.status[i] != COMPLETED:
                self.status_csv.status[i] = MISMATCH

        # Save status
        with data_lock:
            self.status_csv.to_csv(self.status_path)

    # Must be same as DOC_COLUMNS
    def create_record(self,
                      date="",
                      doc_type="",
                      request="",
                      permit="",
                      size="",
                      doc_path=""):

        return [self.ktatrova,
                self.gush,
                self.chelka,
                self.multiple_gush_chelka,
                self.address,
                self.tik_id,
                self.current_page,
                self.current_row,
                date,
                doc_type,
                request,
                permit,
                size,
                doc_path]


class Reporter:

    def __init__(self, ktatrova, input_csv, status_csv):
        self.ktatrova = ktatrova

        # input and status csvs
        self.input_csv = input_csv
        self.status_csv = status_csv
        self.path = ""

        # tiks info
        self.gush_chelka_received = 0
        self.tiks_received = 0

        # Tiks that completed
        self.completed = []
        self.mismatches = []
        self.waiting = []

        # Docs count
        self.docs_in_csv_count = 0
        self.docs_in_dir_count = 0
        self.docs_in_web_count = 0

        # Docs info taken from merged csv for report
        self.gushes = []
        self.chelkas = []
        self.ids = []
        self.pages = []
        self.row_nums = []
        self.docs = []

        # Mismatches
        self.in_csv_not_in_dir = []
        self.in_dir_not_in_csv = []

        # For merged report
        self.reporters = []

    def verify_ktatrova(self):
        res_dir = os.path.join(RESULTS_DIR, self.ktatrova)
        if not os.path.exists(res_dir):
            return

        to_log(self.ktatrova, f"Verifying all files downloaded for Tatrova {self.ktatrova}...")

        # Get all docs in dir, filter the ones the script created
        to_log(self.ktatrova, f"Getting all files in dir for TatRova {self.ktatrova}...")
        dir_docs = glob.glob(f"{res_dir}/**/*.*", recursive=True)
        filtered_dir_docs = [x for x in dir_docs if FILE_PREFIX not in x]

        to_log(self.ktatrova, f"Getting merged csv for TatRova {self.ktatrova}...")
        merged_csv_path = os.path.join(RESULTS_DIR, self.ktatrova, MERGED_CSV_FILE)
        merged_csv = CsvDocHolder()
        merged_csv.read_csv(merged_csv_path)

        self.gush_chelka_received = len([x for x in self.input_csv.ktatrova if x == self.ktatrova])
        self.tiks_received = len(self.status_csv.ktatrova)

        # Get information from merged csv for use in the report
        self.gushes = merged_csv.ms_gush
        self.chelkas = merged_csv.ms_chelka
        self.ids = merged_csv.tik_id
        self.pages = merged_csv.page_number
        self.row_nums = merged_csv.row_number
        self.docs = merged_csv.document

        self.docs_in_csv_count = len(self.docs)
        self.docs_in_dir_count = len(filtered_dir_docs)

        # Sum web count for unique tik_ids
        to_log(self.ktatrova, f"Calculating total web count for TatRova {self.ktatrova}...")
        counted = set()
        for i, tik_id in enumerate(self.status_csv.tik_id):
            if tik_id not in counted:
                self.docs_in_web_count += int(self.status_csv.docs_in_web[i])
            counted.add(tik_id)

        # get mismatches between csv and dir
        to_log(self.ktatrova, f"Calculating mismatches for TatRova {self.ktatrova}...")
        filtered_dir_docs_set = set(filtered_dir_docs)
        docs_set = set(self.docs)
        self.in_csv_not_in_dir = [x for x in enumerate(self.docs) if x[1] not in filtered_dir_docs_set]
        self.in_dir_not_in_csv = [x for x in enumerate(filtered_dir_docs) if x[1] not in docs_set]

        to_log(self.ktatrova, f"Updating completed and mismatches for TatRova {self.ktatrova}...")
        # get info from csv status
        for i in range(len(self.status_csv.ktatrova)):

            # update completed and mismatches in reporter fields
            status_tik = GushChelka(self.status_csv.ktatrova[i],
                                    self.status_csv.ms_gush[i],
                                    self.status_csv.ms_chelka[i],
                                    self.status_csv)
            status_tik.tik_id = self.status_csv.tik_id[i]

            if self.status_csv.status[i] == COMPLETED:
                self.completed.append(status_tik)
            elif self.status_csv.status[i] == MISMATCH:
                self.mismatches.append(status_tik)
            else:
                self.waiting.append(status_tik)

    # Write to TatRova report
    def write_report(self, is_merged_report=False):

        if is_merged_report:
            self.path = os.path.join(RESULTS_DIR, MERGED_REPORT)
            suffix = f"{len(self.reporters)} reports"

        else:
            report_file = f"{FILE_PREFIX}report_TatRova{self.ktatrova}_{CURR_TIME}.txt"
            self.path = os.path.join(RESULTS_DIR, self.ktatrova, report_file)
            suffix = f"TatRova {self.ktatrova}"
            to_log(self.ktatrova, f"Generating report for TatRova {self.ktatrova}...")

        with open(self.path, "a", encoding="utf-8") as v:
            message = f"Summary report for {suffix}\n" \
                      f"\n----------gush-chelka summary----------\n" \
                      f"Total gush-chelka received ({self.gush_chelka_received})\n" \
                      f"Total tiks received ({self.tiks_received})\n" \
                      f"Tiks completed ({len(self.completed)})\n" \
                      f"Tiks with mismatches ({len(self.mismatches)})\n" \
                      f"Tiks waiting ({len(self.waiting)})\n" \
                      f"\n----------Documents summary------------\n" \
                      f"Documents count in csv ({self.docs_in_csv_count})\n" \
                      f"Documents count in directory ({self.docs_in_dir_count})\n" \
                      f"Documents count from web ({self.docs_in_web_count})\n" \
                      f"Documents missing in directory ({len(self.in_csv_not_in_dir)})\n" \
                      f"Documents found in directory but not in csv ({len(self.in_dir_not_in_csv)})\n" \
                      f"\n----------Details----------------------"
            v.write(f"{message}\n")

            if not is_merged_report:
                if self.mismatches:
                    messages = [f"\nThe following {len(self.mismatches)} Tiks have mismatches:"]

                    for tik in self.mismatches:
                        messages.append(f"Ktatrova {tik.ktatrova}, "
                                        f"Gush {tik.gush}, "
                                        f"Chelka {tik.chelka}, "
                                        f"ID {tik.tik_id}")
                    v.write("\n".join(messages))

                if self.completed:
                    messages = [f"\n\nThe following {len(self.completed)} Tiks completed:"]
                    for tik in self.completed:
                        messages.append(f"(Ktatrova {tik.ktatrova}, "
                                        f"Gush {tik.gush}, "
                                        f"Chelka {tik.chelka}, "
                                        f"ID {tik.tik_id}")
                    v.write("\n".join(messages))

                if self.in_csv_not_in_dir:
                    messages = [f"\n\nThe following {len(self.in_csv_not_in_dir)} documents "
                                "found in csv but missing in directory:"]
                    for path in self.in_csv_not_in_dir:
                        prefix = f"(Ktatrova {self.ktatrova}, " \
                                 f"Gush {self.gushes[path[0]]}, " \
                                 f"Chelka {self.chelkas[path[0]]}, " \
                                 f"ID {self.ids[path[0]]}, " \
                                 f"Page {self.pages[path[0]]}, " \
                                 f"Row {self.row_nums[path[0]]})"
                        messages.append(f"{prefix} {path[1]}")
                    v.write("\n".join(messages))

                if self.in_dir_not_in_csv:
                    messages = [f"\n\nThe following {len(self.in_dir_not_in_csv)} documents "
                                "found in directory but not in csv:"]
                    for path in self.in_dir_not_in_csv:
                        messages.append(path[1])
                    v.write("\n".join(messages))

            else:
                if self.reporters:
                    messages = [f"\n\nThe following {len(self.reporters)} reports were merged:"]
                    for reporter in self.reporters:
                        messages.append(f"{reporter.path}")
                    v.write("\n".join(messages))

            to_log(self.ktatrova, f"Finished report for TatRova {self.ktatrova}...")
            v.flush()


class CsvTikHolder:

    def __init__(self):
        self.ktatrova = []
        self.ms_gush = []
        self.ms_chelka = []
        self.tik_id = []
        self.status = []
        self.no_results = []
        self.docs_in_csv = []
        self.docs_in_directory = []
        self.docs_in_web = []
        self.docs_in_csv_not_in_dir = []
        self.docs_in_dir_not_in_csv = []

        # Hold mapping from key to index
        self.key_to_index = {}
        self.next_index = 0

        # Hold all queued tik_ids indexes
        self.tik_ids_queued = set()

    def read_csv(self, path):
        if not os.path.exists(path):
            return

        csv_input = pandas.read_csv(path, converters={i: str for i in range(0, 30)})
        for field in csv_input:
            values = csv_input[field]
            if field == KTATROVA_FIELD:
                self.ktatrova = [x.strip() for x in values]
            elif field == GUSH_FIELD:
                self.ms_gush = [x.strip() for x in values]
            elif field == CHELKA_FIELD:
                self.ms_chelka = [x.strip() for x in values]
            elif field == "tik_id":
                self.tik_id = [x.strip() for x in values]
            elif field == "status":
                self.status = [x.strip() for x in values]
            elif field == NO_RESULTS:
                self.no_results = [x.strip() for x in values]
            elif field == "docs_in_csv":
                self.docs_in_csv = [x.strip() for x in values]
            elif field == "docs_in_directory":
                self.docs_in_directory = [x.strip() for x in values]
            elif field == "docs_in_web":
                self.docs_in_web = [x.strip() for x in values]
            elif field == "docs_in_csv_not_in_dir":
                self.docs_in_csv_not_in_dir = [x.strip() for x in values]
            elif field == "docs_in_dir_not_in_csv":
                self.docs_in_dir_not_in_csv = [x.strip() for x in values]

        # Update dict key_to_index for each row for later easy search
        for i in range(len(self.tik_id)):
            self.add_next_key()

    # Update dict key_to_index {key:row index} for later easy search
    def add_next_key(self):
        i = self.next_index
        key = f"{self.ktatrova[i]}_" \
              f"{self.ms_gush[i]}_" \
              f"{self.ms_chelka[i]}_" \
              f"{self.tik_id[i]}"

        self.key_to_index[key] = i
        self.next_index += 1

    def add(self, record):

        # Add record
        itr = iter(record)
        self.ktatrova.append(next(itr))
        self.ms_gush.append(next(itr))
        self.ms_chelka.append(next(itr))
        self.tik_id.append(str(next(itr)))
        self.status.append(next(itr))
        self.no_results.append(next(itr))
        self.docs_in_csv.append(str(next(itr)))
        self.docs_in_directory.append(str(next(itr)))
        self.docs_in_web.append(str(next(itr)))
        self.docs_in_csv_not_in_dir.append(str(next(itr)))
        self.docs_in_dir_not_in_csv.append(str(next(itr)))

        # Maintain dict with {tik_id : tik_status_index} for easy search
        self.add_next_key()

    def extend(self, records):
        for record in records:
            self.add(record)

    def to_csv(self, path):
        records = []
        for i in range(len(self.ktatrova)):
            record = [self.ktatrova[i],
                      self.ms_gush[i],
                      self.ms_chelka[i],
                      self.tik_id[i],
                      self.status[i],
                      self.no_results[i],
                      self.docs_in_csv[i],
                      self.docs_in_directory[i],
                      self.docs_in_web[i],
                      self.docs_in_csv_not_in_dir[i],
                      self.docs_in_dir_not_in_csv[i]]
            records.append(record)

        if records:
            frame = pandas.DataFrame(records)
            frame.columns = TIK_COLUMNS
            frame.to_csv(path, index=False, encoding="utf-8-sig")


class CsvDocHolder:

    def __init__(self):
        self.ktatrova = []
        self.ms_gush = []
        self.ms_chelka = []
        self.multiple_gush_chelka = []
        self.address = []
        self.tik_id = []
        self.page_number = []
        self.row_number = []
        self.date = []
        self.type = []
        self.request = []
        self.permit = []
        self.size = []
        self.document = []

    def read_csv(self, path):

        if not os.path.exists(path):
            return

        csv = pandas.read_csv(path, converters={i: str for i in range(0, 30)})
        for field in csv:
            values = csv[field]
            if field == KTATROVA_FIELD:
                self.ktatrova = [x.strip() for x in values]
            elif field == GUSH_FIELD:
                self.ms_gush = [x.strip() for x in values]
            elif field == CHELKA_FIELD:
                self.ms_chelka = [x.strip() for x in values]
            elif field == "multiple_gush_chelka":
                self.multiple_gush_chelka = [x.strip() for x in values]
            elif field == "address":
                self.address = [x.strip() for x in values]
            elif field == "tik_id":
                self.tik_id = [x.strip() for x in values]
            elif field == "page_number":
                self.page_number = [x.strip() for x in values]
            elif field == "row_number":
                self.row_number = [x.strip() for x in values]
            elif field == "date":
                self.date = [x.strip() for x in values]
            elif field == "type":
                self.type = [x.strip() for x in values]
            elif field == "request":
                self.request = [x.strip() for x in values]
            elif field == "permit":
                self.permit = [x.strip() for x in values]
            elif field == "size":
                self.size = [x.strip() for x in values]
            elif field == "document":
                self.document = [x.strip() for x in values]

    def to_csv(self, path):
        if not path:
            return

        records = []
        for i in range(len(self.ktatrova)):
            record = [self.ktatrova[i],
                      self.ms_gush[i],
                      self.ms_chelka[i],
                      self.multiple_gush_chelka[i],
                      self.address[i],
                      self.tik_id[i],
                      self.page_number[i],
                      self.row_number[i],
                      self.date[i],
                      self.type[i],
                      self.request[i],
                      self.permit[i],
                      self.size[i],
                      self.document[i]]
            records.append(record)

        if not records:
            records.append([""] * len(DOC_COLUMNS))

        frame = pandas.DataFrame(records)
        frame.columns = DOC_COLUMNS
        frame.to_csv(path, index=False, encoding="utf-8-sig")

    def add(self, record):

        itr = iter(record)
        self.ktatrova.append(next(itr))
        self.ms_gush.append(next(itr))
        self.ms_chelka.append(next(itr))
        self.multiple_gush_chelka.append(next(itr))
        self.address.append(next(itr))
        self.tik_id.append(next(itr))
        self.page_number.append(next(itr))
        self.row_number.append(next(itr))
        self.date.append(next(itr))
        self.type.append(next(itr))
        self.request.append(next(itr))
        self.permit.append(next(itr))
        self.size.append(next(itr))
        self.document.append(next(itr))

    def extend(self, other):
        self.ktatrova.extend(other.ktatrova)
        self.ms_gush.extend(other.ms_gush)
        self.ms_chelka.extend(other.ms_chelka)
        self.multiple_gush_chelka.extend(other.multiple_gush_chelka)
        self.address.extend(other.address)
        self.tik_id.extend(other.tik_id)
        self.page_number.extend(other.page_number)
        self.row_number.extend(other.row_number)
        self.date.extend(other.date)
        self.type.extend(other.type)
        self.request.extend(other.request)
        self.permit.extend(other.permit)
        self.size.extend(other.size)
        self.document.extend(other.document)


def main():
    global CHROME_DRIVER
    global CORES
    global INPUT_FILES
    global RESULTS_DIR

    parser = argparse.ArgumentParser(
        description='Download TLV documents from https://archive-binyan.tel-aviv.gov.il')
    parser.add_argument('--chrome', help="Chrome driver location")
    parser.add_argument('--cores', default=CORES, type=int, help="Number of cores to use")
    parser.add_argument('--input', default=INPUT_FILES, help="Input csv files separated with comma")
    parser.add_argument('--output', default=RESULTS_DIR, help="Output directory")
    args = parser.parse_args()

    CHROME_DRIVER = args.chrome
    CORES = args.cores
    INPUT_FILES = args.input.split(",")
    RESULTS_DIR = args.output

    # combine all input csv files
    combined_input_csv_path, ktatrovas = merge_input_csvs()

    combined_input_csv = CsvTikHolder()
    combined_input_csv.read_csv(combined_input_csv_path)

    # parse status csvs of ktatrovas from input csv
    status_csvs = read_status_csvs(combined_input_csv_path, ktatrovas)

    # Download all docs and get data using threads
    activate_threads(combined_input_csv, status_csvs)

    # Merge csvs
    merge_csvs(ktatrovas)

    # Generate reports for all TatRova
    reporters = verify_and_generate_reports(combined_input_csv, status_csvs, ktatrovas)

    # Generate merge-reports and finish
    merge_reports(combined_input_csv, reporters)


if __name__ == '__main__':
    main()
