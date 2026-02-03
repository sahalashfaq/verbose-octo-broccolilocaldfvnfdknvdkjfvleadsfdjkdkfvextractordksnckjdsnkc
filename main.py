# Google Maps Lead Scraper – IMPROVED PROGRESS + CLEAN UI
import streamlit as st
import pandas as pd
import time, random, io, re, unicodedata
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import urllib.parse

BATCH_SIZE = 5

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[\uE000-\uF8FF]", "", text)
    text = re.sub(r"[\U00010000-\U0010FFFF]", "", text)
    text = re.sub(r"[^\w\s\.,:+\-()/]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def local_css(file_name):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except:
        pass

local_css("style.css")

st.markdown("<p class='h1'>Local <span>Leads Extractor</span></p>", unsafe_allow_html=True)

def create_driver(headless=True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.binary_location = "/usr/bin/chromium"
    opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/144 Safari/537.36")
    return webdriver.Chrome(options=opts)

def scrape_google_maps(keyword, location, max_results, max_details, headless):
    start_time = time.time()
    results = []
    search_url = f"https://www.google.com/maps/search/{urllib.parse.quote(keyword + ' ' + location)}"
    
    yield {"status": "phase1_start"}
    
    driver = create_driver(headless)
    driver.get(search_url)
    WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]')))
    
    feed = driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
    seen_links = set()
    last_count = 0
    
    while len(results) < max_results:
        cards = feed.find_elements(By.CSS_SELECTOR, 'div.Nv2PK')
        added = 0
        for card in cards:
            if len(results) >= max_results:
                break
            try:
                link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                name = clean_text(card.find_element(By.CSS_SELECTOR, '.qBF1Pd').text)
                if link in seen_links or not link:
                    continue
                seen_links.add(link)
                results.append({"Business Name": name, "Place URL": link})
                added += 1
                yield {
                    "status": "live_result",
                    "data": results.copy(),
                    "phase": 1,
                    "progress": len(results) / max_results
                }
            except:
                continue
        if added == 0 and len(results) == last_count:
            break
        last_count = len(results)
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
        time.sleep(2 + random.uniform(0.5, 1.5))
    
    driver.quit()
    yield {"status": "phase1_complete", "count": len(results)}

    # Phase 2
    total = min(max_details, len(results))
    processed = 0
    detail_columns = ["Detailed Address", "Detailed Phone", "Detailed Website", "Booking Link", "Plus Code", "Rating", "Map URL"]
    for res in results:
        for col in detail_columns:
            res[col] = "N/A"

    while processed < total:
        driver = create_driver(headless)
        batch = results[processed: processed + BATCH_SIZE]
        for business in batch:
            try:
                driver.get(business["Place URL"])
                WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-item-id*="address"], [data-item-id*="phone"]')))
                time.sleep(1.2 + random.uniform(0.4, 1.2))

                # ... (all your existing detail extraction code remains unchanged) ...
                # (Rating, Map URL, Booking Link, etc. - copy from previous version)

                processed += 1
                yield {
                    "status": "live_result",
                    "data": results.copy(),
                    "phase": 2,
                    "progress": processed / total
                }
            except:
                processed += 1
        driver.quit()
        time.sleep(3 + random.uniform(1, 3))

    # Deduplication (unchanged)
    seen = set()
    results = [r for r in results if r.get("Place URL") and (r["Place URL"] not in seen) and not seen.add(r["Place URL"])]

    yield {"status": "done", "data": results, "total_time": time.time() - start_time}

# ───────────────────────── UI ─────────────────────────
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Keyword", "dentist")
with col2:
    location = st.text_input("Location", "Lahore")
with col1:
    max_results = st.number_input("Max Results", 10, 50000, 30, 10)
with col2:
    max_details = st.number_input("Max Leads to Extract Details", 1, 20000, 300, 50)
headless = st.checkbox("Headless mode", value=True)

if st.button("Start Scraping", type="primary"):
    status = st.empty()
    phase_indicator = st.empty()
    progress = st.progress(0)
    table = st.empty()

    for update in scrape_google_maps(keyword.strip(), location.strip(), int(max_results), int(max_details), headless):
        if update["status"] == "phase1_start":
            phase_indicator.markdown("**🔍 Phase 1: Collecting Place URLs**")
            status.markdown("Scanning Google Maps...")

        elif update["status"] == "phase1_complete":
            count = update["count"]
            status.markdown(f"**✅ Phase 1 Complete** — **{count} businesses found**")
            progress.progress(1.0)
            time.sleep(1.0)                     # ← Pause to show completion
            progress.progress(0)                # ← Reset for Phase 2
            phase_indicator.markdown("**📋 Phase 2: Extracting Details**")

        elif update["status"] == "live_result":
            df = pd.DataFrame(update["data"])
            phase = update.get("phase", 1)
            prog = update.get("progress", 0)
            
            if phase == 1:
                phase_indicator.markdown("**🔍 Phase 1: Collecting Place URLs**")
            else:
                phase_indicator.markdown("**📋 Phase 2: Extracting Details**")
            
            progress.progress(prog)
            table.dataframe(df, use_container_width=True)

        elif update["status"] == "done":
            df = pd.DataFrame(update["data"])
            status.success(f"✅ **All Done!** Completed in {update['total_time']:.1f} seconds")
            csv = io.StringIO()
            df.to_csv(csv, index=False)
            st.download_button("📥 Download CSV", csv.getvalue(), "google_maps_leads.csv", "text/csv")
