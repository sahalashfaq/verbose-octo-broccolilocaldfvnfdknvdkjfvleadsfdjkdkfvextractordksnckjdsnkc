# Google Maps Lead Scraper – TRUE BATCH ISOLATION + ETA (2026)
# Shows real-time Estimated Time of Arrival (remaining time)

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

st.markdown("<p class='h1'>Local <span>Leads Extractor</span></p>", unsafe_allow_html=True)

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

# ───────────────────────── Human readable time ─────────────────────────
def seconds_to_human(sec):
    if sec < 0:
        return "calculating..."
    if sec < 60:
        return f"{int(sec)} sec"
    if sec < 3600:
        m = int(sec // 60)
        s = int(sec % 60)
        return f"{m} min {s} sec"
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    return f"{h} h {m} min"


# ───────────────────────── SCRAPER with ETA ─────────────────────────
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

        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
        time.sleep(1.8 + random.uniform(0.3, 1.2))

        # ── ETA calculation (place collection phase) ──
        now = time.time()
        processed = len(results) - last_count
        elapsed = now - last_time

        if processed > 0 and elapsed > 0.5:
            speed = processed / elapsed          # items per second
            remaining_items = max_results - len(results)
            eta_sec = remaining_items / speed
            eta_str = seconds_to_human(eta_sec)
            pct = min(100, int(len(results) / max_results * 100))

            yield {
                "status": "info",
                "message": f"Collecting places: {len(results)} / {max_results} ({pct}%) • ETA: {eta_str}"
            }

            last_count = len(results)
            last_time = now

    driver.quit()

    yield {"status": "info", "message": f"Place URLs collected ({len(results)}). Now extracting details…" }

    # ───────────────────────── DETAIL EXTRACTION PHASE ─────────────────────────
    total_to_process = min(max_details, len(results))
    processed = 0

    last_count_details = 0
    last_time_details = time.time()

    st.markdown("It will take 10-30s between each Batch!")
    while processed < total_to_process:
        batch_start_time = time.time()

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

                time.sleep(1.0 + random.uniform(0.4, 1.1))

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

        # ── ETA calculation (detail extraction phase) ──
        batch_time = time.time() - batch_start_time
        now = time.time()

        processed_this_batch = min(BATCH_SIZE, total_to_process - (processed - len(batch)))
        if processed_this_batch > 0:
            speed = processed_this_batch / batch_time if batch_time > 0 else 0
        else:
            speed = 0

        remaining_items = total_to_process - processed
        eta_sec = remaining_items / speed if speed > 0 else -1

        pct = min(100, int(processed / total_to_process * 100)) if total_to_process > 0 else 0

        eta_str = seconds_to_human(eta_sec)

        yield {
            "status": "info",
            "message": f"Detail extraction: {processed}/{total_to_process} ({pct}%) • ETA: {eta_str}"
        }

        time.sleep(3.5 + random.uniform(1, 3))   # anti-ban delay between batches

    total_time = time.time() - start_time

    yield {
        "status": "done",
        "data": results,
        "total_time": total_time
    }


# ───────────────────────── UI ─────────────────────────
col1, col2 = st.columns(2)

with col1:
    keyword = st.text_input("Keyword", "dentist")

with col2:
    location = st.text_input("Location", "Lahore")

with col1:
    max_results = st.number_input(
        "Max Results (scroll limit)",
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
    progress_bar = st.progress(0)
    table_placeholder = st.empty()

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
            df_live = pd.DataFrame(update["data"])
            count = len(df_live)

            # Progress: based on detail extraction phase (most important)
            progress = min(count / max_details, 1.0) if max_details > 0 else 0
            progress_bar.progress(progress)

            table_placeholder.dataframe(
                df_live,
                use_container_width=True,
                hide_index=True
            )

        elif update["status"] == "done":
            df = pd.DataFrame(update["data"])
            status.success(f"Completed successfully in {update['total_time']:.1f} seconds total.")

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            st.download_button(
                label="Download CSV",
                data=csv_buffer.getvalue(),
                file_name="google_maps_leads.csv",
                mime="text/csv"
            )
