# Google Maps Lead Scraper – ENHANCED VERSION (Phase separation + Rating + Map URL + Fixed Booking)
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

# ───────────────────────── CSS (unchanged) ─────────────────────────
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

# ───────────────────────── DRIVER (unchanged) ─────────────────────────
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

# ───────────────────────── TIME FORMAT (unchanged) ─────────────────────────
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
    search_url = f"https://www.google.com/maps/search/{urllib.parse.quote(keyword + ' ' + location)}"
    yield {"status": "info", "message": "Scanning Google Maps and stabilizing results feed… (Phase 1)"}
    driver = create_driver(headless)
    driver.get(search_url)
    WebDriverWait(driver, 40).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
    )
    st.markdown("It will take 10-30s between each Batch!")
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
                results.append({
                    "Business Name": name,
                    "Place URL": link,
                })
                added += 1
                yield {"status": "live_result", "data": results.copy()}
            except:
                continue
        if added == 0 and len(results) == last_count:
            break  # No more new results — Google limit reached
        last_count = len(results)
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
        time.sleep(2 + random.uniform(0.5, 1.5))
    driver.quit()

    found_msg = f"Only {len(results)} results found (Google's per-search limit ≈120). " if len(results) < max_results else ""
    yield {"status": "info", "message": f"{found_msg}Place URLs collected. Extracting details… (Phase 2)"}

    total = min(max_details, len(results))
    processed = 0
    detail_columns = [
        "Detailed Address", "Detailed Phone", "Detailed Website",
        "Booking Link", "Plus Code", "Rating", "Map URL"
    ]
    # Initialize detail fields only when phase 2 starts
    for res in results:
        for col in detail_columns:
            res[col] = "N/A"

    while processed < total:
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
                time.sleep(1.2 + random.uniform(0.4, 1.2))

                # Parse lat/long from Place URL for Map URL
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
                    business["Map URL"] = "N/A"

                try:
                    business["Detailed Address"] = clean_text(
                        driver.find_element(By.CSS_SELECTOR, '[data-item-id*="address"]').text
                    )
                except:
                    pass
                try:
                    business["Detailed Phone"] = clean_text(
                        driver.find_element(By.CSS_SELECTOR, '[data-item-id*="phone"]').text
                    )
                except:
                    pass
                try:
                    business["Detailed Website"] = driver.find_element(
                        By.CSS_SELECTOR, '[data-item-id*="authority"]'
                    ).get_attribute("href")
                except:
                    pass
                try:
                    business["Plus Code"] = clean_text(
                        driver.find_element(By.CSS_SELECTOR, '[data-item-id*="oloc"]').text
                    )
                except:
                    pass

                # Improved Booking Link detection
                business["Booking Link"] = "N/A"
                try:
                    booking_el = driver.find_element(By.CSS_SELECTOR, '[data-item-id^="action:book"], [aria-label*="Book"], [aria-label*="Reserve"], [data-item-id*="reservation"]')
                    href = booking_el.get_attribute("href") or booking_el.get_attribute("data-url")
                    if href and "book" in href.lower() or "reserve" in href.lower():
                        business["Booking Link"] = href
                except:
                    pass

                # Rating using your provided XPath
                try:
                    rating_el = driver.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[9]/div[8]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[1]/span[1]')
                    business["Rating"] = clean_text(rating_el.text)
                except:
                    try:
                        # Fallback common class (more stable)
                        business["Rating"] = clean_text(
                            driver.find_element(By.CSS_SELECTOR, '.F7nice span[aria-hidden="true"]').text
                        )
                    except:
                        pass

                processed += 1
                yield {"status": "live_result", "data": results.copy()}
            except TimeoutException:
                processed += 1
            except Exception:
                processed += 1
        driver.quit()
        time.sleep(3 + random.uniform(1, 3))

    # Final deduplication (just in case)
    seen = set()
    unique_results = []
    for r in results:
        url = r.get("Place URL")
        if url and url not in seen:
            seen.add(url)
            unique_results.append(r)
    results = unique_results

    yield {
        "status": "done",
        "data": results,
        "total_time": time.time() - start_time
    }

# ───────────────────────── UI (unchanged layout) ─────────────────────────
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
    progress = st.progress(0)
    table = st.empty()

    for update in scrape_google_maps(
        keyword.strip(),
        location.strip(),
        int(max_results),
        int(max_details),
        headless
    ):
        if update["status"] == "info":
            status.info(update["message"])
        elif update["status"] == "live_result":
            df = pd.DataFrame(update["data"])
            progress.progress(min(len(df) / max_details, 1.0) if max_details > 0 else 0)
            table.dataframe(df, use_container_width=True)
        elif update["status"] == "done":
            df = pd.DataFrame(update["data"])
            status.success(f"Completed in {seconds_to_human(update['total_time'])}")
            csv = io.StringIO()
            df.to_csv(csv, index=False)
            st.download_button(
                "Download CSV",
                csv.getvalue(),
                "google_maps_leads.csv",
                "text/csv"
            )
