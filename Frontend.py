# importing packages
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy.engine.url import URL

# connecting to the postgresql database engine
db_info = {
    "drivername": "postgresql+psycopg2",
    "username": "postgres",
    "password": "postgres123",
    "host": "super-open-stocks-rds.c9ko248kcabw.us-east-1.rds.amazonaws.com",
    "port": "5432",
    "database": "stock_data"
}
db_url = URL.create(**db_info)
engine = create_engine(db_url)

# setting the dashboard configuration to wide
st.set_page_config(layout = "wide")
# initializing the input field for the stock ticker in the dashboard
ticker = st.sidebar.text_input("## Enter Stock Ticker:").upper()

# checking if stock ticker is not empty
if ticker != "":
    # getting the company info as a dataframe from amazon rds
    company_info_df = pd.read_sql_query(f"""
                            SELECT *
                            FROM company_info
                            WHERE ticker = '{ticker}';
                        """, engine)
    # storing the company name, industry, description and company website as strings
    name = company_info_df["company_nm"][0]
    industry = company_info_df["industry"][0]
    description = company_info_df["company_info"][0]
    company_website = company_info_df["website"][0]
    # getting the company financial statements from amazon rds
    financial_statements_df = pd.read_sql_query(f"""
                            SELECT *
                            FROM financial_statements
                            WHERE ticker = '{ticker}';
                        """, engine)
    # getting the company financial ratios from amazon rds
    ratios_df = pd.read_sql_query(f"""
                            SELECT *
                            FROM ratios
                            WHERE ticker = '{ticker}';
                        """, engine)
    # getting the company stock price values from amazon rds
    stock_price_df = pd.read_sql_query(f"""
                            SELECT *
                            FROM stock_price
                            WHERE ticker = '{ticker}';
                        """, engine)
    # converting the date feature into a datetime datatype
    stock_price_df["month"] = pd.to_datetime(stock_price_df["month"])
    stock_price_df["month"] = stock_price_df["month"].dt.strftime("%b %Y")
    # getting a dataframe of industry average values
    industry_averages = pd.read_sql_query(f"""
                            SELECT AVG(financial_statements.cash_and_cash_equivalents) AS cash_and_cash_equivalents, AVG(financial_statements.ebitda) AS ebitda, AVG(financial_statements.net_income) AS net_income, AVG(financial_statements.net_debt) AS net_debt, AVG(ratios.current_ratio) AS current_ratio, AVG(ratios.free_cash_flow) AS free_cash_flow, AVG(ratios.operating_cash_flow) AS operating_cash_flow, AVG(ratios.debt_to_equity) AS debt_to_equity, AVG(ratios.return_on_assets) AS return_on_assets, AVG(ratios.return_on_equity) AS return_on_equity, AVG(ratios.ev_to_ebitda) AS ev_to_ebitda, AVG(ratios.trailing_pe) AS trailing_pe
                            FROM company_info
                                LEFT JOIN financial_statements
                                    ON company_info.ticker = financial_statements.ticker
                                        LEFT JOIN ratios
                                            ON financial_statements.ticker = ratios.ticker
                            WHERE industry = '{industry}'
                            GROUP BY industry;
                        """, engine)
    # getting the average stock price values for the industry for each month
    industry_average_stock_price = pd.read_sql_query(f"""
                        SELECT stock_price.month, AVG(stock_price.closing_price) AS closing_price
                        FROM company_info
                            LEFT JOIN stock_price
                                ON company_info.ticker = stock_price.ticker
                        WHERE company_info.industry = '{industry}'
                        GROUP BY stock_price.month
                        ORDER BY stock_price.month;
                        """, engine)
    # converting the date feature into a datetime datatype
    industry_average_stock_price["month"] = pd.to_datetime(industry_average_stock_price["month"])
    industry_average_stock_price["month"] = industry_average_stock_price["month"].dt.strftime("%b %Y")
    # joining the ratios and financial statement values into one dataframe
    financial_statements_and_ratios = pd.merge(financial_statements_df, ratios_df.drop(columns = "current_ratio"), left_index = True, right_index = True, how = "left")
    # conducting a union between the industry averages and the company values for financial statements and ratios
    industry_and_company = pd.concat([financial_statements_and_ratios, industry_averages])
    # setting the index to be labelled as either the company ticker or the industry average
    industry_and_company.index = [ticker, "Industry Average"]
    # dropping columns which are not needed for analysis
    industry_and_company.drop(columns = ["ticker_x", "total_debt", "current_assets", "current_liabilities", "ticker_y", "outstanding_shares", "latest_closing_price", "dividend_yield", "market_cap"], inplace = True)
    # filtering for only financial statement values
    financial_statements_df = industry_and_company[["cash_and_cash_equivalents", "ebitda", "net_income", "net_debt", "free_cash_flow", "operating_cash_flow"]]
    # filtering for only financial ratio values
    ratios_df = industry_and_company[["current_ratio", "trailing_pe", "debt_to_equity", "return_on_assets", "return_on_equity", "ev_to_ebitda"]]
    # transposing the dataframes above
    financial_statements_df = financial_statements_df.T
    ratios_df = ratios_df.T

    # inserting the dashboard sidebar information
    st.sidebar.title(f"{name}")
    st.sidebar.markdown("## Industry")
    st.sidebar.markdown(f"{industry}")
    st.sidebar.markdown("## Company Website")
    st.sidebar.markdown(f"{company_website}")
    st.sidebar.markdown("## Description")
    st.sidebar.markdown(f"{description}")

    # inserting the main streamlit dashboard title
    st.title(f"{name} Stock Investing Guide")
    # creating the introductory text for the dashboard
    st.markdown("""
                The purpose of this dashboard is to provide financial information about companies for individuals who are not familiar with finance. This dashboard contains information about stock prices and the most important metrics for evaluating a company's financial situation compared to the industry.
                """)
    # creating the disclaimer text
    st.markdown("*Disclaimer: This is not financial advice and is solely for educational purposes.*")

    # creating a line chart for stock price changes over time
    trace_stock = go.Scatter(x = stock_price_df["month"], y = stock_price_df["closing_price"], mode = "lines", name = ticker, line = dict(color = "#D6B4FC", width = 3))
    trace_industry_average = go.Scatter(x = industry_average_stock_price["month"], y = industry_average_stock_price["closing_price"], mode = "lines", name = "Industry Average", line = dict(color = "#FBF07B", width = 3))
    layout = go.Layout(showlegend = True)
    fig = go.Figure(data = [trace_stock, trace_industry_average], layout = layout)
    fig.update_layout(barmode = "group", title = f"{ticker} Stock Price Over Time",
                xaxis = dict(title = "Date", showgrid = False),
                yaxis = dict(title = "USD ($)", showgrid = False),
                title_x = 0.3,
                title_font = dict(size = 24))
    st.plotly_chart(fig, use_container_width = 600)

    # creating a bar chart for financial statement comparisons between industry and the stock
    trace1 = go.Bar(x = financial_statements_df.index, y = financial_statements_df[ticker], name = ticker, marker = dict(color = "#D6B4FC"))
    trace2 = go.Bar(x = financial_statements_df.index, y = financial_statements_df["Industry Average"], name = "Industry Average", marker = dict(color = "#FBF07B"))
    fig_financial_statements_chart = go.Figure(data = [trace1, trace2])
    fig_financial_statements_chart.update_layout(barmode = "group", title = f"{ticker} vs. Industry Average Financial Statement Values",
                    yaxis = dict(title = "USD ($)", showgrid = False),
                    xaxis = dict(showgrid = False, tickvals = list(range(6)), ticktext = ["Cash and Cash Equivalents", "EBITDA", "Net Income", "Net Debt", "Free Cash Flow", "Operating Cash Flow"]),
                    title_x = 0.2,
                    title_font = dict(size = 24))
    st.plotly_chart(fig_financial_statements_chart, use_container_width = 600)

    # creating descriptions for financial statements values and valuable insights for them
    st.markdown("#### Cash and Cash Equivalents")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company holding cash and cash equivalents above the industry average suggests a strong liquidity position, offering financial flexibility, enhanced risk management, and the ability to capitalize on growth opportunities without relying on external financing. This can signal to the market and investors that the company is financially stable and strategically poised for future investments or shareholder value enhancement. However, it's essential to balance this against potential perceptions of missed investment opportunities or inefficient capital use, as excessively high cash reserves may also raise concerns about the company's growth prospects and operational efficiency.
        """)
    st.markdown("#### EBITDA")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company reporting an EBITDA (Earnings Before Interest, Taxes, Depreciation, and Amortization) above the industry average typically indicates operational efficiency and profitability. This superior EBITDA margin suggests that the company is generating substantial earnings from its operations before accounting for interest, taxes, and non-cash expenses, highlighting its core business strength. Such financial performance can attract investors by showcasing the company's ability to generate cash flow, invest in growth opportunities, and withstand economic fluctuations. However, while high EBITDA is often a positive sign, it's crucial to consider it alongside other financial metrics to fully assess a company's financial health and long-term sustainability.
        """)
    st.markdown("#### Net Income")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company reporting a net income above the industry average demonstrates a strong profitability and financial health, indicating it effectively manages its expenses and operations to maximize earnings. This superior profitability can signal to investors and stakeholders the company's success in translating revenues into actual profits, after accounting for all costs, including taxes and interest. Such financial performance suggests not only efficient operations but also the potential for sustainable growth, dividend payouts, and investment in new opportunities. High net income can enhance the company's appeal to investors, potentially leading to a higher stock price. However, it's essential to analyze this metric in the context of the company's overall financial strategy and market conditions, as singular emphasis on net income without considering other financial health indicators might not provide a complete picture.
        """)
    st.markdown("#### Net Debt")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company reporting a net debt below the industry average suggests a strong balance sheet characterized by low leverage, which indicates the company has managed its borrowing prudently and maintains a healthy ratio of debt to equity. This position can signal financial stability and flexibility, as the company relies less on external financing and faces lower interest obligations, potentially freeing up more resources for investment, growth opportunities, and returns to shareholders. A lower net debt level can also provide a buffer against economic downturns and financial stress, enhancing the company’s ability to navigate adverse conditions without jeopardizing its operational integrity. However, it's crucial to consider the industry context and the company's growth strategy, as too little debt might also suggest an overly conservative approach that could limit growth opportunities and the efficient use of capital.
        """)
    st.markdown("#### Free Cash Flow")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company with a free cash flow (FCF) above the industry average showcases its ability to generate cash from its operations after accounting for capital expenditures, which is critical for sustaining growth, paying dividends, and reducing debt. This strong FCF indicates not only operational efficiency but also financial flexibility, allowing the company to invest in new opportunities, innovate, and return value to shareholders without relying on external financing. High free cash flow is often seen by investors as a sign of a company’s health and potential for long-term success, as it reflects the company's capability to generate surplus cash that can be used strategically. However, it's important to analyze this in conjunction with the company's investment plans, as consistently high FCF could also suggest underinvestment in the business, potentially stifling future growth.
        """)
    st.markdown("#### Operating Cash Flow")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company that reports operating cash flow above the industry average demonstrates strong operational efficiency and an effective cash conversion cycle. This indicates the company's core business activities generate more cash than its peers, providing essential liquidity for day-to-day operations, debt repayment, investment, and growth initiatives without depending on external financing. High operating cash flow signals to investors and stakeholders the company's ability to sustain and grow its operations profitably, highlighting a solid foundation for financial health and potential value creation. However, it's crucial to view this metric in the context of the company's overall financial strategy and investment needs, as reinvestment in the business is essential for long-term growth, and excessively high operating cash flow might reflect underinvestment.
        """)

    # creating a bar chart for financial ratio comparisons between industry and the stock
    trace3 = go.Bar(x = ratios_df.index, y = ratios_df[ticker], name = ticker, marker = dict(color = "#D6B4FC"))
    trace4 = go.Bar(x = ratios_df.index, y = ratios_df["Industry Average"], name = "Industry Average", marker = dict(color = "#FBF07B"))
    fig_ratio_chart = go.Figure(data = [trace3, trace4])
    fig_ratio_chart.update_layout(barmode = "group", title = f"{ticker} vs. Industry Average Financial Ratio Values",
                    yaxis = dict(title = "USD ($)", showgrid = False),
                    xaxis = dict(showgrid = False, tickvals = list(range(6)), ticktext = ["Current Ratio", "PE Ratio", "Debt to Equity", "Return on Assets (ROA)", "Return on Equity (ROE)", "EV to EBITDA"]),
                    title_x = 0.2,
                    title_font = dict(size = 24))
    st.plotly_chart(fig_ratio_chart, use_container_width = 600)

    # creating descriptions for financial ratio values and valuable insights for them
    st.markdown("#### Current Ratio")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company with a current ratio above the industry average indicates strong liquidity, meaning it possesses more than enough short-term assets (like cash, inventory, and receivables) to cover its short-term liabilities (such as accounts payable and other upcoming financial obligations). This higher current ratio suggests the company is well-positioned to meet its short-term financial commitments, reflecting financial stability and operational efficiency. It can also signal to investors and creditors that the company manages its working capital effectively and has a buffer against financial distress, enhancing its creditworthiness. However, it's essential to balance; a too-high current ratio might indicate an excessive accumulation of assets or underutilization of resources that could otherwise be invested for growth. Therefore, while a higher current ratio is generally positive, it should be considered alongside other financial metrics and operational goals to assess the company's overall financial health and strategic direction accurately.
        """)
    st.markdown("#### PE Ratio")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company with a Price-to-Earnings (P/E) ratio above the industry average suggests that its stock is priced higher relative to its earnings than its peers, which can imply that investors expect higher growth rates, profitability, or stability from this company in the future. This elevated P/E ratio indicates a market perception of the company as having strong future prospects, possibly due to innovative products, market leadership, or efficient operations. It reflects investor confidence in the company's ability to generate profits above and beyond the industry norm. However, a high P/E ratio also raises questions about overvaluation and the sustainability of the growth rates required to justify such valuation. It's important for investors to consider this metric in the context of the company's growth potential, sector dynamics, and broader market conditions to evaluate whether the stock is a worthwhile investment or if it poses a risk of being overvalued.
        """)
    st.markdown("#### Debt to Equity Ratio")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company with a debt-to-equity (D/E) ratio above the industry average indicates it relies more heavily on debt financing relative to equity to fund its operations and growth. This higher leverage can suggest the company is aggressive in its growth strategy, potentially offering higher returns on equity due to the use of borrowed funds. However, a high D/E ratio also implies greater risk, as the company must ensure it can cover its debt obligations, even in adverse economic conditions. This reliance on debt can make the company more vulnerable to interest rate increases or downturns in its business cycle, affecting its profitability and financial stability. Investors and creditors often scrutinize a high D/E ratio to assess the balance between seeking higher growth through leverage and the risks associated with increased debt levels. Therefore, while a high D/E ratio may signal growth ambitions, it also calls for careful evaluation of the company's ability to manage its debt responsibly and maintain financial health.
        """)
    st.markdown("#### Return on Assets (ROA)")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company with a Return on Assets (ROA) ratio above the industry average demonstrates superior efficiency in utilizing its assets to generate profits. This higher ROA indicates that the company is effectively converting its investments in assets into net income, showcasing operational excellence and strategic management of resources. It reflects not just the company’s profitability but also its ability to leverage its asset base for maximum financial performance. Such efficiency can attract investors looking for businesses that deliver strong returns on their investments. However, while a high ROA is a positive indicator of financial health and operational efficiency, it's essential to consider this metric in the broader context of the company's financial strategies, industry conditions, and growth prospects. A significantly high ROA, compared to peers, may also prompt further investigation to ensure it is sustainable and not the result of aggressive accounting practices.
        """)
    st.markdown("#### Return on Equity (ROE)")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company with a Return on Equity (ROE) ratio above the industry average is seen as achieving superior efficiency in generating profits from its shareholders' equity. This indicates that the company is effectively using its invested capital to produce earnings, reflecting strong management performance and financial health. A high ROE suggests that the company is capable of generating significant income on the investment made by its shareholders, which can be particularly appealing to investors looking for businesses that offer high returns on their investments. However, it's important to analyze the components and sources of a high ROE to ensure it's driven by genuine operational efficiency and growth, rather than by high levels of debt which can inflate ROE but also introduce higher financial risk. A sustainable high ROE, not overly reliant on debt, indicates a robust business model and operational effectiveness, positioning the company favorably for future growth and profitability.
        """)
    st.markdown("#### EV to EBITDA Ratio")
    st.markdown(
        """
        If the company values are greater than the industry average:

        A company with an Enterprise Value to EBITDA (EV/EBITDA) ratio above the industry average might be perceived as having a higher valuation compared to its earnings before interest, taxes, depreciation, and amortization. This can suggest that the market values the company more highly, possibly due to expectations of future growth, operational efficiencies, or strong market position. A higher EV/EBITDA ratio indicates investors are willing to pay more for each dollar of EBITDA generated by the company, which could reflect its competitive advantages, potential for market expansion, or superior management practices. However, it's also important to be cautious, as a significantly high ratio compared to industry peers might indicate overvaluation, where the price paid for the company's earnings and cash flow is excessively high relative to its actual financial performance. This metric should therefore be considered in conjunction with other financial analyses and market conditions to gauge whether the company's higher valuation is justified by its growth prospects and operational strengths.
        """)

    # initializing the footer text for the dashboard
    st.markdown("**Created by Shabbir Khandwala, Gurdeep Panag, Lukas Escoda and Harjot Dhaliwal for DATA 608. All rights reserved.**")

else:
    # if there is no stock ticker inputted, show this message
    st.title(f"Please Input a Valid Stock Ticker")
    # putting an introduction paragraph
    st.markdown("""
                The purpose of this dashboard is to provide financial information about companies for individuals who are not familiar with finance. This dashboard contains information about stock prices and the most important metrics for evaluating a company's financial situation compared to the industry.
                """)
    # putting the text for the disclaimer
    st.markdown("*Disclaimer: This is not financial advice and is solely for educational purposes.*")
    
    # ____________________________________________________________________________________________________________________________
    # creating the motivation button
    gif_path = "motivation.gif"
    if "show_gif" not in st.session_state:
        st.session_state.show_gif = False
    button = st.button("Click for Motivation")
    if button:
        st.session_state.show_gif = not st.session_state.show_gif 
    if st.session_state.show_gif:
        st.image(gif_path, width = 800)
    # ____________________________________________________________________________________________________________________________

    # initializing the footer text for the dashboard
    st.markdown("**Created by Shabbir Khandwala, Gurdeep Panag, Lukas Escoda and Harjot Dhaliwal for DATA 608. All rights reserved.**")