import sys

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import argparse
from config import Config

from bs4 import BeautifulSoup
import pandas as pd
import time
import io
import re
import json
import math
import subprocess


#chrome.exe --remote-debugging-port=9222 --user-data-dir="D:\Antony\chrome-profile"
class ScreenerFundamentalExtractor:

    def __init__(self):
        print("Starting Chrome with remote debugging...")
        chrome_options = Options()
        chrome_options.debugger_address = "127.0.0.1:9222"
        self.driver = webdriver.Chrome(options=chrome_options)
        print("Connected to Chrome.")
        df_units = pd.read_csv("unitConfig.csv")
        internal_units = df_units["unit"].tolist()
        self.llmUnit = dict(zip(df_units["unit"], df_units["llm_unit"]))
        
        escaped_units = sorted(
                            [re.escape(u) for u in internal_units],
                            key=len,
                            reverse=True
                        )

        self.pattern = r"(" + "|".join(escaped_units) + r")$"

    def open_company(self, symbol):

        url = f"https://www.screener.in/company/{symbol}/consolidated/"

        self.driver.get(url)

        time.sleep(3)

    def get_html(self):

        return self.driver.page_source

    def parse_ratios(self, soup):

        ratios = {}

        ul = soup.select_one("#top-ratios")

        if not ul:
            return ratios

        items = ul.find_all("li")

        for item in items:

            name = item.find("span", class_="name")
            value = item.find("span", class_="number")

            if name and value:

                ratios[name.text.strip()] = value.text.strip()

        return ratios

    def extractTableGlobalUnit(self, section):
        if not section:
            return None

        elem = section.select_one("div.flex-row.flex-space-between.flex-gap-16 > div:nth-child(1) > p")
        if not elem:
            return None

        text = elem.get_text(strip=True)
        match = re.search(r'(Rs\.|USD)\s*(Crores|Lakhs|Millions)', text)
        unit = match.group() if match else None

        return unit

    def parse_table(self, soup, section_id):

        section = soup.find("section", id=section_id)
        table = section.find("table", class_="data-table")
        if not table:
            return None

        df = pd.read_html(io.StringIO(str(table)))[0]
        df.iloc[:,0] = (
            df.iloc[:,0]
            .str.replace("\xa0", " ", regex=False)
            .str.replace("+", "", regex=False)
            .str.strip()
        )
        df["unit"] = self.extractTableGlobalUnit(section)
        return df
    
    def extractUnitFromData(self, metricsNames):
        # dict mapping
        unit_map = {
            #quaterly results, Profit & Loss
            "OPM": "%",
            "TAX": "%",
            "Tax": "%",
            "EPS": "Rs",
            "Dividend Payout": "%",
            
            #Ratios
            "Debtor Days": "Days",
            "Inventory Days": "Days",
            "Days Payable": "Days",
            "Cash Conversion Cycle": "Days",
            "Working Capital Days": "Days",
            "ROCE": "%",

            # insights
            "Installed Production Capacity": "Kg",
            "Actual Production Volume": "Kg",
            "Capacity Utilization": "%",
            "New Facility Land Area": "Sq. Mtrs",
            "R&D Specialist Strength": "Number",
            "Revenue Concentration - Top 10 Customers": "%",
            "Revenue Concentration - Top 5 Customers": "%",
            "Total Employee Strength": "Number",
            "Working Capital Cycle": "Days",
            "Number of Branches Number": "Number",
            "Home Loan Market Share": "%",
            "Transactions through alternate channels": "%",
            "Auto Loan Market Share": "%",
            "Registered Users on YONO": "Number in Crore",
            "CASA Market Share": "%",
            "Domestic Deposit Market Share": "%",
            

            # Shareholding Pattern
            "FIIs": "%",
            "DIIs": "%",
            "Promoters": "%",
            "Public": "%",
            "No. of Shareholders": "Number",
            "Number of Shareholders": "Number",
            "Government": "%",
            "Others": "%",
        }

        # Step 1: detect unit
        unit = next((u for k,u in unit_map.items() if k in metricsNames), None)
        if unit:
            return unit
        

        
        match = re.search(self.pattern, metricsNames)

        return self.llmUnit.get(match.group(0)) if match else None

    def screener_table_to_json(self,df, table_name, reportNoneUnits=True):

        # first column is metric names
        metric_col = df.columns[0]
        

        result = {
            "table": table_name,
            "periods": list(df.columns[1:-1]),
            "metrics": {}
        }

        for _, row in df.iterrows():

            metric = row[metric_col]

            values = {}
            unit = row["unit"]
            for col in df.columns[1:-1]:
                val = row[col]

                try:
                    val_str = str(val).strip()  # e.g., "12.5%" or "1,234.56"
                    unit = self.extractUnitFromData(metric) or unit
                    val = float(str(val_str).replace('%','').replace(',',''))
                except:
                    val = None

                values[col] = val

            result["metrics"][metric] = {
                "unit": unit,
                "values": values
            }
            if reportNoneUnits and unit is None:
                print(f"❌ Error: No unit detected for metric '{metric}' in table '{table_name}'")

        return result

    def extract_metrics(self, symbol):

        self.open_company(symbol)
        html = self.get_html()
        soup = BeautifulSoup(html, "html.parser")
        #ratios = self.parse_ratios(soup)
        quarterly = self.parse_table(soup, "quarters")
        pnl = self.parse_table(soup, "profit-loss")
        balancesheet = self.parse_table(soup, "balance-sheet")
        cashflow = self.parse_table(soup, "cash-flow")
        ratios = self.parse_table(soup, "ratios")
        insights = self.parse_table(soup, "insights")
        shareholding = self.parse_table(soup, "shareholding")

        quarterly_json = self.screener_table_to_json(quarterly,"results")
        pnl_json = self.screener_table_to_json(pnl, "profit-loss")
        balancesheet_json = self.screener_table_to_json(balancesheet, "balance-sheet")
        cashflow_json = self.screener_table_to_json(cashflow, "cash-flow")
        ratios_json = self.screener_table_to_json(ratios, "ratios")
        insights_json = self.screener_table_to_json(insights, "insights")
        shareholding_json = self.screener_table_to_json(shareholding, "shareholding")

        

        stock_data = {
            "stock": symbol,
            "tables": [
                quarterly_json,
                pnl_json,
                balancesheet_json,
                cashflow_json,
                ratios_json,
                insights_json,
                shareholding_json
            ]
        }

        return stock_data
def replace_nan(obj):
    if isinstance(obj, dict):
        return {k: replace_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan(x) for x in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    else:
        return obj

def starRemoteChromeBrowser():
    # Define the path to the Chrome executable (adjust if necessary for your installation)
    # Common paths on Windows are:
    # "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
    # "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

    # Define the user data directory path
    user_data_dir = r"D:\Antony\chrome-profile"

    # Ensure no other instances of Chrome are running with the same profile, as this can cause conflicts.
    # You might want to manually close all Chrome instances before running the script.

    # Command and arguments as a list
    command_args = [
        chrome_path,
        f"--remote-debugging-port=9222",
        f"--user-data-dir={user_data_dir}"
    ]

    print(f"Starting Chrome with command: {' '.join(command_args)}")

    try:
        # Start the Chrome process
        # Use Popen if you want the script to continue running while Chrome is open
        process = subprocess.Popen(command_args)
        
        # Optional: Keep the script alive for a few seconds or until the process is manually terminated
        # print("Chrome started. Waiting for 10 seconds before potentially continuing the script...")
        # time.sleep(10) 
        # process.terminate() # to close the browser after a certain time
        
    except FileNotFoundError:
        print(f"Error: Chrome executable not found at {chrome_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


def llmPrompt(stock, financial_json):
    prompt = f"""
        Act as a professional growth stock analyst using Mark Minervini's Superperformance (SEPA) framework.
        You must ONLY use the financial data provided in the JSON below.
        Do NOT invent numbers.
        If a metric is missing, state "Data Not Available".

        All conclusions must reference the provided metrics.
        Stock: {stock}
        Financial Data: {financial_json}

        The JSON dataset is the single source of truth for all analysis.
        
        ---

        ### Step 1: Data Architecture

        1. Detect whether the dataset contains:

        * Annual results (e.g., Mar 2023)
        * Quarterly results (e.g., Jun 2024)

        2. If both exist:

        * Use **annual data for long-term structural analysis**
        * Use **quarterly data for acceleration detection**
        
        Important Rules:

        1. Use ONLY the numbers present in the dataset. Do not create new numbers.
        2. Do NOT invent or modify numeric values.
        3. If a required metric is missing, return "Data Not Available".
        4. Do NOT infer metrics that are not explicitly provided, except when the prompt explicitly instructs a fallback (e.g., use debtor days if CCC is unavailable).
        5. Every numeric statement must match a value from the dataset.
        6. Derived metrics (e.g., CAGR, ratios) may be calculated ONLY if all required input values are present in the dataset.
        7. When interpreting growth, momentum, or any financial signal, always tie it back to the specific values in the dataset. For example, if you say "Revenue is growing at a strong pace," you must reference the actual revenue figures and their growth rates from the data.
        8. The final verdict and scoring must be based solely on the metrics present in the dataset.
        9. Treat null, empty, or missing values as "Data Not Available".
        10. Treat placeholders such as "-", "—", "NA", or "N/A" as "Data Not Available".

        ---

        ### Step 2: Metric Extraction & Interpretation

        Calculate the following using the most recent **3 - 5 years of annual data** when available.

        #### 1. Growth & Momentum

        * Revenue CAGR
        * Net Profit CAGR
        * EPS CAGR (if available)

        Acceleration Check:
        Compare the **latest 2 quarters vs previous 4 quarters** to determine if growth is accelerating.

        ---

        #### 2. Advanced Multibagger Signals

        **Order Book Visibility**
        If order book data exists:

        Order Book / TTM Revenue

        Interpretation:

        * <1x → Weak visibility
        * 1 - 2x → Normal pipeline
        * 2 - 3x → Strong growth visibility
        * > 3x → Elite growth visibility

        If unavailable → mark as **Data Not Available**.

        ---

        **Structural Margin Expansion**

        Analyze multi-year Operating Margin (OPM%).

        Example expansion pattern:
        8% → 11% → 15%

        Classify as:

        * Expanding
        * Stable
        * Contracting

        ---

        **Working Capital Efficiency**

        Analyze trend in **Cash Conversion Cycle (CCC)** or debtor days.

        Large decline in CCC indicates improving efficiency.

        Classify as:

        * Strong Improvement
        * Stable
        * Deteriorating

        If CCC unavailable → infer from debtor days trend.

        ---

        **Growth Inflection**

        Detect pattern:

        Stage 1:
        Multi-year stagnant revenue.

        Stage 2:
        Sudden strong growth breakout.

        If detected → mark **Growth Inflection Detected**.

        ---

        #### 3. Institutional & Financial Health

        Evaluate:

        Ownership:

        * Promoter holding trend
        * FII participation
        * DII / mutual fund presence

        Balance Sheet:

        * Debt to Equity
        * Interest Coverage

        Determine whether growth is:

        * Self-funded
        * Debt-driven

        ---

        ### Step 3: Minervini Superperformance Scoring (/50)

        Growth Strength (0–15)

        Revenue CAGR Scoring Guide:

        > 25% → 5 points
        15–25% → 3 points
        < 15% → 1 point
        Data Not Available → 0 points

        Earnings CAGR Scoring Guide:

        > 25% → 5 points
        15–25% → 3 points
        < 15% → 1 point
        Data Not Available → 0 points

        Quarterly Acceleration:

        Clear acceleration → 3 points
        Stable growth → 2 points
        No acceleration → 1 point
        Data Not Available → 0 points

        Growth Inflection Signal:

        Detected → 2 points
        Not detected → 0 points

        Profitability Quality (0–10)

        * OPM strength
        * Margin expansion
        * ROCE consistency

        Balance Sheet / Efficiency (0–10)

        * Debt levels
        * Interest coverage
        * Working capital efficiency

        Institutional Interest (0–10)

        * Promoter trend
        * FII/DII participation

        Consistency (0–5)

        * Earnings stability
        * Penalize large profit volatility

        ---

        ### Step 3.5: Verification

        Before producing the final answer:

        1. Verify every number mentioned exists in the dataset.
        2. Ensure derived metrics use only provided values.
        3. If a required metric is missing, mark "Data Not Available".
        4. Do not introduce any external information about the company.

        ---
        ### Step 4: Final Output (Strict Format)

        Detected Data Structure:

        * Annual: Yes / No
        * Quarterly: Yes / No

        Key Financial Observations:
        • Observation 1
        • Observation 2
        • Observation 3

        Advanced Growth Signals:
        • Order Book: [Value]x – [Insight]
        • Margins: [Trend] – [Insight]
        • Efficiency: [CCC Trend] – [Insight]
        • Inflection: [Detected / Not Detected] – [Reason]

        Superperformance Score: X / 50

        Verdict:
        Elite (≥40) / Watchlist (30–39) / Avoid (<30)

        Short Explanation:
        • Reason 1
        • Reason 2
        • Reason 3
        Output ONLY the sections defined above.
        Do not add any additional commentary or explanation.
    """
    return prompt

def generatePrompts(df):
    starRemoteChromeBrowser()
    results = []
    extractor = ScreenerFundamentalExtractor()
    symbols = df["Symbol"].unique()   # only unique entries
    for idx, symbol in enumerate(symbols):

        try:
            # 1️⃣ Extract fundamental JSON
            print(f"<=====Processing {symbol}...")
            fundamental_json = extractor.extract_metrics(symbol)

            # 2️⃣ Generate prompt
            prompt = llmPrompt(symbol, fundamental_json)

            # 3️⃣ Store result
            results.append({
                "index": idx,
                "symbol": symbol,
                "prompt": prompt
            })

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    # 4️⃣ Convert to dataframe
    result_df = pd.DataFrame(results)

    # 5️⃣ Save to CSV
    result_df.to_csv(Config.BASE_OUTPUT_DIR / "watch/llm_prompts.csv", index=False)

    print("Saved prompts to llm_prompts.csv")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Visualize Dailly, Weekly Monthly charts for selected symbols"
    )

    parser.add_argument("--symbol-file", type=str, help=f"Name of the symbol file located in {Config.TMP_DIR} directory", default=None)
    # Show help if no arguments provided
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()  

if __name__ == "__main__":
    args = parse_args()
    if args.symbol_file:
        filepath = Config.TMP_DIR / args.symbol_file
        if filepath.exists():
            df = pd.read_csv(filepath, usecols=["Symbol"])
            generatePrompts(df)
        
        