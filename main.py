# Google Maps Lead Scraper – TRUE BATCH ISOLATION + ETA (Streamlit Cloud SAFE)

import streamlit as st
import pandas as pd
import time, random, io
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

BATCH_SIZE = 5

# ───────────────────────── CSS ─────────────────────────
def local_css(file_name):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except:
        pass

local_css("style.css")

st.markdown(
    "<p class='h1'>Local <span>Leads Extractor</span></p>",
    unsafe_allow_html=True
)

# ───────────────────────── DRIVER (FIXED) ─────────────────────────
def create_driver(headless=True):
    opts = Options()

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    # IMPORTANT: Streamlit Cloud Chromium
    opts.binary_location = "/usr/bin/chromium"

    opts.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/144 Safari/537.36"
    )

    # Selenium Manager auto-handles driver
    return webdriver.Chrome(options=opts)

# ───────────────────────── Time Helper ─────────────────────────
def seconds_to_human(sec):
    if sec < 0:
        return "calculating..."
    if sec < 60:
        return f"{int(sec)} sec"
    if sec < 3600:
        return f"{int(sec // 60)} min {int(sec % 60)} sec"
    return f"{int(sec // 3600)} h {int((sec % 3600) // 60)} min"

# ───────────────────────── SCRAPER ─────────────────────────
def scrape_google_maps(keyword, location, max_results, max_details, headless):

    start_time = time.time()
    results = []

    search_url = f"https://www.google.com/maps/search/{keyword}+{location}".replace(" ", "+")

    yield {"status": "info", "message": "Scanning Google Maps and stabilizing results feed…"}

    driver = create_driver(headless)
    driver.get(search_url)

    WebDriverWait(driver, 40).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
    )

    feed = driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
    seen_links = set()

    last_count = 0
    last_time = time.time()

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

        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight", feed
        )
        time.sleep(1.5 + random.uniform(0.3, 1.0))

        now = time.time()
        processed = len(results) - last_count
        elapsed = now - last_time

        if processed > 0 and elapsed > 0.5:
            speed = processed / elapsed
            remaining = max_results - len(results)
            eta = seconds_to_human(remaining / speed)
            pct = int(len(results) / max_results * 100)

            yield {
                "status": "info",
                "message": f"Collecting places: {len(results)}/{max_results} ({pct}%) • ETA: {eta}"
            }

            last_count = len(results)
            last_time = now

    driver.quit()

    yield {
        "status": "info",
        "message": f"Place URLs collected ({len(results)}). Now extracting details…"
    }

    # ───────────────────────── DETAIL PHASE ─────────────────────────
    total = min(max_details, len(results))
    processed = 0

    while processed < total:
        yield {
            "status": "info",
            "message": f"Extracting details (batch {processed // BATCH_SIZE + 1})…"
        }

        driver = create_driver(headless)
        batch = results[processed: processed + BATCH_SIZE]

        for business in batch:
            try:
                driver.get(business["Place URL"])

                WebDriverWait(driver, 25).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '[data-item-id*="address"], [data-item-id*="phone"]')
                    )
                )

                time.sleep(1 + random.uniform(0.3, 1.0))

                try:
                    business["Detailed Address"] = driver.find_element(
                        By.CSS_SELECTOR, '[data-item-id*="address"]'
                    ).text.strip()
                except: pass

                try:
                    business["Detailed Phone"] = driver.find_element(
                        By.CSS_SELECTOR, '[data-item-id*="phone"]'
                    ).text.strip()
                except: pass

                try:
                    business["Detailed Website"] = driver.find_element(
                        By.CSS_SELECTOR, '[data-item-id*="authority"]'
                    ).get_attribute("href")
                except: pass

                try:
                    business["Plus Code"] = driver.find_element(
                        By.CSS_SELECTOR, '[data-item-id*="oloc"]'
                    ).text.strip()
                except: pass

                business["Booking Link"] = business["Detailed Website"]
                processed += 1

                yield {"status": "live_result", "data": results.copy()}

            except TimeoutException:
                processed += 1

        driver.quit()
        time.sleep(3 + random.uniform(1, 3))

    yield {
        "status": "done",
        "data": results,
        "total_time": time.time() - start_time
    }

# ───────────────────────── UI ─────────────────────────
col1, col2 = st.columns(2)

with col1:
    keyword = st.text_input("Keyword", "dentist")

with col2:
    location = st.text_input("Location", "Lahore")

with col1:
    max_results = st.number_input(
        "Max Results",
        min_value=10,
        max_value=50000,
        value=80,
        step=10
    )

with col2:
    max_details = st.number_input(
        "Max Leads to Extract Details",
        min_value=1,
        max_value=20000,
        value=300,
        step=50
    )

headless = st.checkbox("Headless mode", value=True)

if st.button("Start Scraping", type="primary"):
    status = st.empty()
    progress = st.progress(0)
    table = st.empty()

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
                f"<div style='padding:10px;background:#1e293b;color:#e2e8f0;border-radius:6px;'>{update['message']}</div>",
                unsafe_allow_html=True
            )

        elif update["status"] == "live_result":
            df = pd.DataFrame(update["data"])
            progress.progress(min(len(df) / max_details, 1.0))
            table.dataframe(df, use_container_width=True, hide_index=True)

        elif update["status"] == "done":
            df = pd.DataFrame(update["data"])
            status.success(f"Completed in {update['total_time']:.1f} seconds")

            csv = io.StringIO()
            df.to_csv(csv, index=False)
            st.download_button(
                "Download CSV",
                csv.getvalue(),
                "google_maps_leads.csv",
                "text/csv"
            )
