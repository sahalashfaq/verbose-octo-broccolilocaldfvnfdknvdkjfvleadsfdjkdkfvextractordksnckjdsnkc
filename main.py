# Google Maps Lead Scraper – FINAL VERSION WITH N/A STYLING + PERFECT UI
import streamlit as st
import pandas as pd
import time
import random
import io
import re
import unicodedata
import urllib.parse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

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

# ───────────────────────── SCRAPER ─────────────────────────
def scrape_google_maps(keyword, location, max_results, max_details, headless):
    start_time = time.time()
    results = []
    search_url = f"https://www.google.com/maps/search/{urllib.parse.quote(keyword + ' ' + location)}"

    yield {"status": "phase1_start"}

    driver = create_driver(headless)
    driver.get(search_url)
    try:
        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
        )
    except TimeoutException:
        yield {"status": "error", "message": "Failed to load Google Maps results. Try again later."}
        driver.quit()
        return

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
                if not link or link in seen_links:
                    continue
                seen_links.add(link)
                results.append({"Business Name": name, "Place URL": link})
                added += 1
                yield {
                    "status": "live_result",
                    "data": results.copy(),
                    "phase": 1,
                    "progress": min(len(results) / max_results, 1.0)
                }
            except:
                continue

        if added == 0 and len(results) == last_count:
            break
        last_count = len(results)
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
        time.sleep(2.0 + random.uniform(0.5, 1.5))

    driver.quit()
    yield {"status": "phase1_complete", "count": len(results)}

    # Phase 2
    total = min(max_details, len(results))
    if total == 0:
        yield {"status": "done", "data": results, "total_time": time.time() - start_time}
        return

    detail_columns = [
        "Address", "Phone", "Provided Website link",
        "Provided Booking Link", "Plus Code", "Rating"
    ]
    for res in results:
        for col in detail_columns:
            res[col] = "N/A"

    processed = 0
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

                # Address
                try:
                    business["Address"] = clean_text(
                        driver.find_element(By.CSS_SELECTOR, '[data-item-id*="address"]').text
                    )
                except:
                    pass

                # Phone
                try:
                    business["Phone"] = clean_text(
                        driver.find_element(By.CSS_SELECTOR, '[data-item-id*="phone"]').text
                    )
                except:
                    pass

                # Website
                try:
                    business["Provided Website link"] = driver.find_element(
                        By.CSS_SELECTOR, '[data-item-id*="authority"]').get_attribute("href")
                except:
                    pass

                # Plus Code
                try:
                    business["Plus Code"] = clean_text(
                        driver.find_element(By.CSS_SELECTOR, '[data-item-id*="oloc"]').text
                    )
                except:
                    pass

                # Booking Link
                business["Provided Booking Link"] = "N/A"
                try:
                    booking = driver.find_element(
                        By.CSS_SELECTOR,
                        '[data-item-id^="action:book"], [aria-label*="Book"], [aria-label*="Reserve"]'
                    )
                    href = booking.get_attribute("href") or ""
                    if href and any(k in href.lower() for k in ["book", "reserve", "appointment"]):
                        business["Booking Link"] = href
                except:
                    pass

                # Rating
                try:
                    rating_el = driver.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[9]/div[8]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[1]/span[1]')
                    business["Rating"] = clean_text(rating_el.text)
                except:
                    try:
                        business["Rating"] = clean_text(
                            driver.find_element(By.CSS_SELECTOR, '.F7nice span[aria-hidden="true"]').text
                        )
                    except:
                        pass

            except Exception:
                pass

            processed += 1
            yield {
                "status": "live_result",
                "data": results.copy(),
                "phase": 2,
                "progress": min(processed / total, 1.0)
            }

        driver.quit()
        time.sleep(3.5 + random.uniform(1.0, 2.5))

    # Final deduplication
    seen = set()
    unique_results = [r for r in results if r.get("Place URL") and r["Place URL"] not in seen and not seen.add(r["Place URL"])]
    results = unique_results

    yield {"status": "done", "data": results, "total_time": time.time() - start_time}

# ───────────────────────── UI ─────────────────────────
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Keyword", "seo expert")
with col2:
    location = st.text_input("Location", "Lahore")

col1, col2 = st.columns(2)
with col1:
    max_results = st.number_input("Max Results (Phase 1)", 10, 50000, 120, 10)
with col2:
    max_details = st.number_input("Max Details to Extract (Phase 2)", 1, 20000, 300, 50)

headless = st.checkbox("Headless mode", value=True)

if st.button("Start Scraping", type="primary"):
    status = st.empty()
    phase_indicator = st.empty()
    progress_bar = st.progress(0)
    table_placeholder = st.empty()

    # Styling function for N/A
    def style_na(val):
        if str(val).strip() == "N/A":
            return 'color: #aaaaaa; font-style: italic;'
        else:
            return 'color: #000000; font-weight: 500;'

    for update in scrape_google_maps(keyword.strip(), location.strip(), int(max_results), int(max_details), headless):
        if update["status"] == "phase1_start":
            phase_indicator.markdown("**Phase 1 – Collecting business links**")
            status.markdown("Scanning Google Maps...")

        elif update["status"] == "phase1_complete":
            count = update["count"]
            status.markdown(f"**Phase 1 complete** — found **{count}** places")
            progress_bar.progress(1.0)
            time.sleep(0.8)
            progress_bar.progress(0)
            phase_indicator.markdown("**Phase 2 – Extracting details**")
            status.markdown("Visiting each place page to get phone, address, website...")

        elif update["status"] == "live_result":
            df = pd.DataFrame(update["data"])
            phase = update.get("phase", 1)
            prog = update.get("progress", 0)
            progress_bar.progress(prog)

            if phase == 1:
                cols_to_show = ["Business Name", "Place URL"]
                display_df = df[cols_to_show] if all(c in df.columns for c in cols_to_show) else df
                table_placeholder.dataframe(display_df, use_container_width=True)
            else:
                styled_df = df.style.applymap(style_na)
                table_placeholder.dataframe(styled_df, use_container_width=True)

        elif update["status"] == "done":
            df = pd.DataFrame(update["data"])
            final_styled = df.style.applymap(style_na)
            table_placeholder.dataframe(final_styled, use_container_width=True)

            phase_indicator.empty()
            status.success(
                f"**All done!** \n"
                f"Extracted details for **{len(df)}** leads in {update['total_time']:.1f} seconds"
            )
            csv = io.StringIO()
            df.to_csv(csv, index=False)
            st.download_button(
                label="Download CSV",
                data=csv.getvalue(),
                file_name="google_maps_leads.csv",
                mime="text/csv"
            )

        elif update.get("status") == "error":
            status.error(update["message"])





