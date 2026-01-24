# WRDS CRSP/Compustat Pipeline

This pipeline downloads and processes CRSP stock returns and Compustat fundamentals from WRDS, and replicates the Fama-French (1993) factors.

## Data Source

- CRSP Monthly Stock File (via WRDS)
- Compustat Fundamentals Annual (via WRDS)
- CRSP-Compustat Link Table (via WRDS)

## Outputs

- `ftsfr_CRSP_monthly_stock_ret.parquet`: Monthly stock returns (including dividends)
- `ftsfr_CRSP_monthly_stock_retx.parquet`: Monthly stock returns (excluding dividends)
- `FF_1993_factors.parquet`: Replicated SMB and HML factors
- `FF_1993_Comparison.png`: Comparison plot with Ken French Data Library

## Usage

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up credentials:
   ```bash
   cp .env.example .env
   # Edit .env with your WRDS username
   ```

3. Run the pipeline:
   ```bash
   doit
   ```

4. View the generated documentation in `docs/index.html`

## Data Coverage

- Time period: 1926 - present
- Frequency: Monthly
- Granularity: Individual stocks

## Fama-French Factor Construction

Factors are constructed following the methodology in Fama-French (1993):
- SMB (Small Minus Big): Size factor
- HML (High Minus Low): Value factor

Uses NYSE breakpoints for portfolio formation.

## Academic References

### Primary Paper

- **Fama and French (1993)** - "Common risk factors in the returns on stocks and bonds"
  - Journal of Financial Economics 33 (1993): 3-56
  - Foundational paper for SMB and HML factor construction

### Key Findings Replicated

- Small stocks (low market equity) have higher average returns than large stocks
- High book-to-market stocks have higher average returns than low book-to-market stocks
- The three-factor model explains most cross-sectional variation in stock returns

### Validation

Unit tests compare our replicated factors against the Ken French Data Library, achieving correlations >0.98.
