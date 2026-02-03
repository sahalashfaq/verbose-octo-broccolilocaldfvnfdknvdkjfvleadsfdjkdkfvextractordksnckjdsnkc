# Google Maps Lead Scraper – TRUE BATCH ISOLATION (2026)
# UI/UX Enhancement: Loader + Progress + Status Text (NO LAYOUT CHANGES)

import streamlit as st
import pandas as pd
import time, random, io
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

BATCH_SIZE = 5
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

local_css("style.css")
st.markdown("<p class='h1'>Local <span>Leads Extractor</span></p>",unsafe_allow_html=True)
# ───────────────────────── DRIVER ─────────────────────────
def create_driver(headless=True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128 Safari/537.36"
    )
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )

# ───────────────────────── SCRAPER (UNCHANGED) ─────────────────────────
def scrape_google_maps(keyword, location, max_results, max_details, headless):

    start_time = time.time()
    results = []

    search_url = f"https://www.google.com/maps/search/{keyword}+{location}".replace(" ", "+")

    yield {"status": "info", "message": "Scanning Google Maps and stabilizing results feed…" }

    driver = create_driver(headless)
    driver.get(search_url)

    WebDriverWait(driver, 40).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
    )

    feed = driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
    seen_links = set()

    while len(results) < max_results:
        cards = feed.find_elements(By.CSS_SELECTOR, 'div.Nv2PK')

        for card in cards:
            if len(results) >= max_results:
                break
            try:
                link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                name = card.find_element(By.CSS_SELECTOR, '.qBF1Pd').text.strip()
                if link in seen_links:
                    continue
                seen_links.add(link)

                results.append({
                    "Business Name": name,
                    "Place URL": link,
                    "Detailed Address": "N/A",
                    "Detailed Phone": "N/A",
                    "Detailed Website": "N/A",
                    "Booking Link": "N/A",
                    "Plus Code": "N/A",
                })

                yield {"status": "live_result", "data": results.copy()}

            except:
                continue

        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
        time.sleep(2)

    driver.quit()

    yield {"status": "info", "message": "Place URLs collected successfully. Switching to detail extraction…" }

    total_to_process = min(max_details, len(results))
    processed = 0

    while processed < total_to_process:
        batch = results[processed: processed + BATCH_SIZE]

        yield {
            "status": "info",
            "message": f"Extracting verified business details (batch {processed // BATCH_SIZE + 1})…"
        }

        driver = create_driver(headless)

        for business in batch:
            try:
                driver.get(business["Place URL"])

                WebDriverWait(driver, 25).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '[data-item-id*="address"], [data-item-id*="phone"]')
                    )
                )

                time.sleep(1.2)

                try:
                    business["Detailed Address"] = driver.find_element(By.CSS_SELECTOR,'[data-item-id*="address"]').text.strip()
                except: pass

                try:
                    business["Detailed Phone"] = driver.find_element(By.CSS_SELECTOR,'[data-item-id*="phone"]').text.strip()
                except: pass

                try:
                    business["Detailed Website"] = driver.find_element(By.CSS_SELECTOR,'[data-item-id*="authority"]').get_attribute("href")
                except: pass

                try:
                    business["Plus Code"] = driver.find_element(By.CSS_SELECTOR,'[data-item-id*="oloc"]').text.strip()
                except: pass

                business["Booking Link"] = business["Detailed Website"]

                processed += 1
                yield {"status": "live_result", "data": results.copy()}

            except TimeoutException:
                processed += 1
                continue

        driver.quit()
        time.sleep(4 + random.uniform(1, 2))

    yield {
        "status": "done",
        "data": results,
        "total_time": time.time() - start_time
    }
    
col1, col2 = st.columns(2)

with col1:
    keyword = st.text_input("Keyword", "dentist")

with col2:
    location = st.text_input("Location", "Lahore")

with col1:
    max_results = st.number_input(
        "Max Results",
        min_value=5,
        max_value=150000,
        value=30
    )

with col2:
    max_details = st.number_input(
        "Max Leads to Extract",
        min_value=1,
        max_value=100000,
        value=10000
    )

headless = st.checkbox("Headless", True)

if st.button("Start Scraping", type="primary"):

    status = st.empty()
    reassurance = st.empty()
    progress_bar = st.progress(0)
    table = st.empty()

    reassurance.markdown(
        "<div class='subtle'>System initialized. Browser and scraping engine are running normally.</div>",
        unsafe_allow_html=True
    )

    gen = scrape_google_maps(
        keyword.strip(),
        location.strip(),
        int(max_results),
        int(max_details),
        headless
    )

    for update in gen:
        if update["status"] == "info":
            status.markdown(
                f"<div class='status-box'>{update['message']}</div>",
                unsafe_allow_html=True
            )

        elif update["status"] == "live_result":
            count = len(update["data"])
            progress_bar.progress(min(count / max_details, 1.0))

            reassurance.markdown(
                "<div class='subtle'>Scraper is active, stable, and processing data in real time. No action required.</div>",
                unsafe_allow_html=True
            )

            table.dataframe(pd.DataFrame(update["data"]), use_container_width=True)

        elif update["status"] == "done":
            df = pd.DataFrame(update["data"])
            status.success(f"Completed successfully in {update['total_time']:.1f} seconds.")

            csv = io.StringIO()
            df.to_csv(csv, index=False)
            st.download_button("Download CSV", csv.getvalue(), "leads.csv", "text/csv")
