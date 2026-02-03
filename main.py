# Google Maps Lead Scraper – BULK MULTI-LOCATION + PHASE SEPARATION + ALL FEATURES
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

# ───────────────────────── TEXT CLEANER ─────────────────────────
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[\uE000-\uF8FF]", "", text)
    text = re.sub(r"[\U00010000-\U0010FFFF]", "", text)
    text = re.sub(r"[^\w\s\.,:+\-()/]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# ───────────────────────── CSS (keep your style.css) ─────────────────────────
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

# ───────────────────────── DRIVER ─────────────────────────
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
    opts.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/144 Safari/537.36"
    )
    return webdriver.Chrome(options=opts)

# ───────────────────────── TIME FORMAT ─────────────────────────
def seconds_to_human(sec):
    if sec < 0: return "calculating..."
    if sec < 60: return f"{int(sec)} sec"
    if sec < 3600: return f"{int(sec // 60)} min {int(sec % 60)} sec"
    return f"{int(sec // 3600)} h {int((sec % 3600) // 60)} min {int(sec % 60)} sec"

# ───────────────────────── SINGLE SEARCH SCRAPER ─────────────────────────
def scrape_single_search(keyword, location, max_results_per_search, max_details, headless, progress_callback):
    results = []
    search_url = f"https://www.google.com/maps/search/{urllib.parse.quote(keyword + ' ' + location)}"
    yield {"status": "info", "message": f"Phase 1: Scanning '{keyword} {location}' …"}

    driver = create_driver(headless)
    driver.get(search_url)
    try:
        WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]')))
    except:
        yield {"status": "info", "message": f"No feed found for '{location}' — skipping."}
        driver.quit()
        return

    feed = driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
    seen_links = set()
    last_count = 0
    while len(results) < max_results_per_search:
        cards = feed.find_elements(By.CSS_SELECTOR, 'div.Nv2PK')
        added = 0
        for card in cards:
            if len(results) >= max_results_per_search:
                break
            try:
                link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                name = clean_text(card.find_element(By.CSS_SELECTOR, '.qBF1Pd').text)
                if link in seen_links or not link:
                    continue
                seen_links.add(link)
                results.append({"Business Name": name, "Place URL": link})
                added += 1
                progress_callback(results)
            except:
                continue
        if added == 0 and len(results) == last_count:
            break
        last_count = len(results)
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
        time.sleep(2 + random.uniform(0.5, 1.5))
    driver.quit()

    found_msg = f"Only {len(results)} found (Google limit) for '{location}'." if len(results) < max_results_per_search else ""
    yield {"status": "info", "message": f"{found_msg} Phase 2: Extracting details for {min(max_details, len(results))} leads…"}

    detail_columns = ["Detailed Address", "Detailed Phone", "Detailed Website", "Booking Link", "Plus Code", "Rating", "Map URL"]
    for res in results:
        for col in detail_columns:
            res[col] = "N/A"

    processed = 0
    total = min(max_details, len(results))
    while processed < total:
        driver = create_driver(headless)
        batch = results[processed: processed + BATCH_SIZE]
        for business in batch:
            try:
                driver.get(business["Place URL"])
                WebDriverWait(driver, 25).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-item-id*="address"], [data-item-id*="phone"]'))
                )
                time.sleep(1.2 + random.uniform(0.4, 1.2))

                # Map URL from coords in Place URL
                try:
                    parsed = urllib.parse.urlparse(business["Place URL"])
                    for part in parsed.path.split('/'):
                        if part.startswith('@'):
                            coords = part[1:].split(',')
                            if len(coords) >= 2:
                                lat, lng = coords[0], coords[1]
                                business["Map URL"] = f"https://www.google.com/maps/@{lat},{lng},17z"
                                break
                except:
                    pass

                for selector, key in [
                    ('[data-item-id*="address"]', "Detailed Address"),
                    ('[data-item-id*="phone"]', "Detailed Phone"),
                    ('[data-item-id*="authority"]', "Detailed Website"),
                    ('[data-item-id*="oloc"]', "Plus Code")
                ]:
                    try:
                        if key == "Detailed Website":
                            business[key] = driver.find_element(By.CSS_SELECTOR, selector).get_attribute("href")
                        else:
                            business[key] = clean_text(driver.find_element(By.CSS_SELECTOR, selector).text)
                    except:
                        pass

                # Booking Link
                try:
                    booking_el = driver.find_element(By.CSS_SELECTOR, '[data-item-id^="action:book"], [aria-label*="Book"], [aria-label*="Reserve"], [data-item-id*="reservation"]')
                    href = booking_el.get_attribute("href") or booking_el.get_attribute("data-url")
                    if href and ("book" in href.lower() or "reserve" in href.lower()):
                        business["Booking Link"] = href
                except:
                    pass

                # Rating
                try:
                    business["Rating"] = clean_text(driver.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[9]/div[8]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[1]/span[1]').text)
                except:
                    try:
                        business["Rating"] = clean_text(driver.find_element(By.CSS_SELECTOR, '.F7nice span[aria-hidden="true"]').text)
                    except:
                        pass

                processed += 1
                progress_callback(results)
            except:
                processed += 1
        driver.quit()
        time.sleep(3 + random.uniform(1, 3))

    yield {"status": "partial", "data": results}

# ───────────────────────── MAIN BULK SCRAPER ─────────────────────────
def scrape_bulk(keyword, locations, max_results_per_search, max_details, headless):
    start_time = time.time()
    all_results = []
    seen_urls = set()

    status = st.empty()
    table = st.empty()
    progress = st.progress(0)

    total_locations = len(locations)
    for i, loc in enumerate(locations, 1):
        loc = loc.strip()
        if not loc:
            continue
        status.info(f"Processing location {i}/{total_locations}: {loc} … ({len(all_results)} leads so far)")

        def update_table(current_results):
            # Dedup on the fly for display
            unique = []
            seen = set()
            for r in current_results + all_results:
                url = r.get("Place URL")
                if url and url not in seen:
                    seen.add(url)
                    unique.append(r)
            df = pd.DataFrame(unique)
            table.dataframe(df, use_container_width=True)
            progress.progress(min(len(unique) / (max_details * total_locations or 1), 1.0))

        for update in scrape_single_search(keyword, loc, max_results_per_search, max_details, headless, update_table):
            if update["status"] == "info":
                status.info(update["message"])
            elif update["status"] == "partial":
                all_results.extend(update["data"])

    # Final dedup
    unique_results = []
    for r in all_results:
        url = r.get("Place URL")
        if url not in seen_urls:
            seen_urls.add(url)
            unique_results.append(r)

    total_time = time.time() - start_time
    yield {
        "status": "done",
        "data": unique_results,
        "total_time": total_time,
        "message": f"Done! Collected {len(unique_results)} unique leads across {total_locations} locations in {seconds_to_human(total_time)}"
    }

# ───────────────────────── UI ─────────────────────────
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Keyword", "dentist")
with col2:
    location_input = st.text_area(
        "Locations (one per line — e.g. neighborhoods/areas in Lahore)",
        "Lahore\nDHA Lahore\nGulberg Lahore\nJohar Town Lahore\nModel Town Lahore\nCantt Lahore\nSamanabad Lahore\nGarden Town Lahore\nFaisal Town Lahore\nWapda Town Lahore",
        height=150
    )

locations = [line.strip() for line in location_input.split("\n") if line.strip()]

with col1:
    max_results_per_search = st.number_input("Max Results per Location", 10, 500, 120, 10)
with col2:
    max_details = st.number_input("Max Details to Extract (per location)", 1, 20000, 100, 10)

headless = st.checkbox("Headless mode", value=True)

if st.button("Start Bulk Scraping", type="primary"):
    if not locations:
        st.error("Please enter at least one location.")
    else:
        status = st.empty()
        progress = st.progress(0)
        table = st.empty()

        for update in scrape_bulk(
            keyword.strip(),
            locations,
            int(max_results_per_search),
            int(max_details),
            headless
        ):
            if update["status"] in ["info", "partial"]:
                status.info(update.get("message", "Processing..."))
            elif update["status"] == "done":
                df = pd.DataFrame(update["data"])
                status.success(update["message"])
                csv = io.StringIO()
                df.to_csv(csv, index=False)
                st.download_button(
                    "Download All Leads CSV",
                    csv.getvalue(),
                    "google_maps_bulk_leads.csv",
                    "text/csv"
                )
                table.dataframe(df, use_container_width=True)
