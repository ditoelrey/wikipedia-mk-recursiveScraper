

---

# 🇲🇰 Macedonian Wikipedia Async Scraper

> A high-performance, asynchronous web scraper designed to build a comprehensive Macedonian language text corpus for Large Language Model (LLM) training.

## 📋 Project Overview

This tool extracts **clean, plain text** from the Macedonian Wikipedia (`mk.wikipedia.org`). Unlike traditional scrapers that parse raw HTML or XML dumps, this project utilizes the **MediaWiki API** to fetch pre-processed text, ensuring high data quality suitable for Natural Language Processing (NLP) tasks.

The scraper is built with **Python's `asyncio` and `aiohttp**`, allowing for concurrent data fetching that is significantly faster and more efficient than synchronous threading methods.

## 🚀 Key Features

* **⚡ Asynchronous Architecture:** Uses non-blocking I/O to scrape ~50+ articles per second without overloading client resources.
* **🧹 Clean Text Extraction:** Fetches `explaintext` directly from the API, bypassing the need for complex HTML parsing or Regex cleaning of XML tags.
* **🔄 Smart Deduplication:** Implements O(1) deduplication using unique `PageIDs`, ensuring no duplicate data enters the dataset.
* **💾 Resumable Operation:** Maintains a local state (`visited_urls.txt`), allowing the scraper to be stopped and resumed without losing progress.
* **📦 JSONL Output:** Saves data in **JSON Lines** format, the industry standard for streaming large datasets to LLM tokenizers.

## 📊 Dataset Statistics

* **Source:** mk.wikipedia.org (Namespace 0 - Articles)
* **Coverage:** ~147,000 articles (100% of current Macedonian Wikipedia)
* **Format:** `.jsonl` (approx. 450MB raw text)
* **Content:**
* `page_id`: Unique MediaWiki ID
* `title`: Article title
* `text`: Full article body (plain text)
* `categories`: Associated categories
* `url`: Source link



## 🛠️ Installation

1. **Clone the repository:**
```bash
git clone https://github.com/yourusername/mk-wiki-scraper.git
cd mk-wiki-scraper

```


2. **Install dependencies:**
```bash
pip install -r requirements.txt

```


*(Dependencies usually include: `aiohttp`, `aiodns`)*

## ⚙️ Usage & Strategy

This project employs a **Hybrid Strategy** to ensure full data coverage and easy maintenance.

### Phase 1: Initial Full Load (The Baseline)

To collect the entire existing database (~147k articles), the scraper uses an **AllPages Enumeration** strategy (or Recursive Category Crawling). This "walks" through the entire Wikipedia index alphabetically.

```bash
# Run the main scraper
python recursiveScraper.py

```

### Phase 2: Weekly Maintenance (The Delta Update)

To keep the dataset fresh without re-downloading 1GB of data every week, the system supports an **Incremental Update** mode. It queries the `RecentChanges` API endpoint to fetch only articles created or edited in the last 7 days.

* **Efficiency:** ~2 minutes runtime per week.
* **Resource Usage:** Negligible (fetches only ~100-200 new articles).

## 📄 Data Structure

The output is a line-delimited JSON file (`dataset.jsonl`). Each line is a valid JSON object:

```json
{
    "page_id": 1344213,
    "title": "Агенција за вработување на Македонија",
    "url": "https://mk.wikipedia.org/wiki/...",
    "text": "Агенција за вработување на Македонија е институција која...",
    "categories": ["Македонија", "Институции"],
    "scraped_at": "2026-01-21T14:00:00"
}

```

## 🆚 Comparison with Other Scrapers

| Feature | This Project | Standard HTML Scrapers | XML Dump Parsers |
| --- | --- | --- | --- |
| **Speed** | **High** (Async IO) | Medium (Threaded) | High (Offline) |
| **Data Quality** | **Clean Plain Text** | Dirty (HTML tags) | Dirty (Wiki Markup) |
| **Maintenance** | **Easy** (Incremental API) | Hard (Full re-scrape) | Hard (Large downloads) |
| **Storage** | **Optimized** (JSONL) | Multiple JSON files | Massive XML files |

## 🤝 Contributing

Contributions are welcome! If you have ideas for better preprocessing or analysis of the Macedonian text, please open an issue or submit a pull request.

## 📜 License

This project is open-source and available under the **MIT License**.
Data scraped from Wikipedia is subject to the **Creative Commons Attribution-ShareAlike 3.0** license.
