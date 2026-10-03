# Online Retail II — teaching slice v1

## Provenance

- Authoritative archive URL: `https://archive.ics.uci.edu/static/public/502/online%2Bretail%2Bii.zip`
- Dataset page: `https://archive.ics.uci.edu/dataset/502/online%2Bretail%2Bii`
- Retrieved: `2026-09-04`
- Original archive: `45622418` bytes; SHA-256 `572e36277c2390fbfde10664750731e0a86f55e33470d91919085f0408e67bfb`
- Citation: Chen, D. (2012). *Online Retail II* [Dataset]. UCI Machine Learning Repository. DOI: `10.24432/C5CG6D`.
- Licence: CC BY 4.0.

The original archive is an untracked maintainer cache and is deliberately not
included in this repository. Student and checker paths use only the frozen
public files in this directory and do not download data.

## Frozen slice contract

`sample.csv` contains exactly eight UTF-8/LF columns in this order:
`invoice_id`, `stock_code`, `description`, `quantity`, `invoice_at`,
`unit_price`, `customer_id`, `country`. Its `SHA256SUMS` manifest must match
before use.

`quantity` is a signed integer; `unit_price` is a canonical non-negative
decimal with at most six fractional places, quantized with `ROUND_HALF_EVEN`;
`customer_id` is a nullable numeric identifier string; and `invoice_at` is an
ISO 8601 local-naive timestamp.

The v1 transform uses salt `mlsd-2026-online-retail-v1` and threshold `100`,
selects complete invoices deterministically, and preserves cancellation/return
records. The frozen result has 11,116 data rows and 967,677 bytes, within the
published laptop-sized bounds of 10,000–12,000 rows and 900,000–1,100,000
bytes.

## Limitations

This is a deterministic educational sample, not a statistically representative
or current retail population. It preserves source `customer_id` values but
excludes email and address fields. `invoice_at` is a timezone-naive/local-naive
rendering of the source Excel serial time; the upstream workbook does not
establish a timezone.
Amounts and descriptions retain upstream quality limitations, including returns,
cancellations and historical data quality issues.
