# Cara menjalankan

```powershell
# Terminal 1 — engine
uv run python -m uvicorn engine.api.main:app --reload --port 8000

# Terminal 2 — UI
$env:ENGINE_URL = "http://localhost:8000"
uv run python -m streamlit run ui/app.py --server.port 8501
```

UI: http://localhost:8501

---

## 1. Net revenue per channel

**Pertanyaan:** Berapa total net revenue per channel penjualan?

**SQL:**

```sql
SELECT sales_channel, SUM(net_revenue) AS net_revenue
FROM gold.order_360
GROUP BY sales_channel
ORDER BY net_revenue DESC
```

**Tabel yang dipakai:** `gold.order_360`

**Hasil:**

| sales_channel |  net_revenue |
| ------------- | -----------: |
| WEB           | 3.473.000,22 |
| STORE         | 3.386.261,46 |
| MOBILE_APP    | 3.372.631,39 |
| MARKETPLACE   | 3.326.781,76 |

**Penjelasan:** Net revenue dijumlahkan pada grain order — satu order dihitung
sekali, sehingga tidak ada penggandaan nilai.

---

## 2. Refund rate per channel

**Pertanyaan:** Channel mana yang punya refund rate tertinggi?

**SQL:**

```sql
SELECT sales_channel,
       AVG(CASE WHEN refunded_amount > 0 THEN 1.0 ELSE 0.0 END) AS refund_rate
FROM gold.order_360
GROUP BY sales_channel
ORDER BY refund_rate DESC
```

**Tabel yang dipakai:** `gold.order_360`

**Hasil:**

| sales_channel | refund_rate |
| ------------- | ----------: |
| WEB           |      0,2042 |
| MOBILE_APP    |      0,2005 |
| MARKETPLACE   |      0,1970 |
| STORE         |      0,1947 |

**Penjelasan:** Refund rate = proporsi order yang punya refund selesai.
Keempat channel hampir merata — sekitar 19–20%.

---

## 3. AOV per channel

**Pertanyaan:** Berapa rata-rata nilai order (AOV) per channel?

**SQL:**

```sql
SELECT sales_channel, AVG(net_revenue) AS average_order_value
FROM gold.order_360
GROUP BY sales_channel
ORDER BY average_order_value DESC
```

**Tabel yang dipakai:** `gold.order_360`

**Hasil:**

| sales_channel | average_order_value |
| ------------- | ------------------: |
| WEB           |            1.385,32 |
| MOBILE_APP    |            1.352,30 |
| STORE         |            1.345,36 |
| MARKETPLACE   |            1.340,36 |

---

## 4. Revenue per kategori produk

**Pertanyaan:** Kategori produk apa yang menghasilkan revenue terbesar?

**SQL:**

```sql
SELECT active_category, SUM(net_revenue) AS net_revenue
FROM gold.product_daily
GROUP BY active_category
ORDER BY net_revenue DESC
```

**Tabel yang dipakai:** `gold.product_daily`

**Hasil:**

| active_category |  net_revenue |
| --------------- | -----------: |
| fashion         | 3.300.661,87 |
| electronics     | 3.120.805,93 |
| sports          | 2.984.213,34 |
| home            | 2.259.052,50 |
| grocery         | 2.128.875,33 |
| beauty          | 1.269.408,57 |

**Penjelasan:** Kategori diambil dari versi yang **berlaku pada tanggal transaksi**
(SCD), bukan kategori terbaru.

---

## 5. ROAS campaign terbaik

**Pertanyaan:** Campaign mana yang punya ROAS terbaik?

**SQL:**

```sql
SELECT campaign_id,
       SUM(net_revenue) / NULLIF(SUM(campaign_spend), 0) AS roas
FROM gold.channel_campaign_daily
GROUP BY campaign_id
HAVING SUM(campaign_spend) > 0
ORDER BY roas DESC
```

**Tabel yang dipakai:** `gold.channel_campaign_daily`

**Hasil (5 teratas):**

| campaign_id |    roas |
| ----------- | ------: |
| CMP-02      | 52,7695 |
| CMP-06      | 51,6609 |
| CMP-03      | 50,0657 |
| CMP-05      | 48,7516 |
| CMP-09      | 48,2320 |

**Penjelasan:** ROAS = net revenue yang diatribusikan dibagi belanja campaign.
Setiap order masuk ke tepat satu campaign (atribusi round-robin), sehingga
tidak ada order yang dihitung berkali-kali.

---

## 6. Pelanggan aktif per hari

**Pertanyaan:** Berapa pelanggan aktif setiap hari?

**SQL:**

```sql
SELECT metric_date, active_customers
FROM gold.executive_kpis_daily
ORDER BY active_customers DESC
```

**Tabel yang dipakai:** `gold.executive_kpis_daily`

**Hasil (5 teratas):**

| metric_date | active_customers |
| ----------- | ---------------: |
| 2026-07-23  |              131 |
| 2026-07-18  |              131 |
| 2026-09-15  |              130 |
| 2026-09-11  |              130 |
| 2026-09-04  |              126 |

**Penjelasan:** Pelanggan aktif = pelanggan yang punya minimal satu order
pada tanggal tersebut. Tersedia untuk seluruh 90 hari data.

---

## 7. Produk terlaris berdasarkan unit

**Pertanyaan:** Produk apa saja yang paling banyak terjual (unit)?

**SQL:**

```sql
SELECT product_id, SUM(units_sold) AS units_sold
FROM gold.product_daily
GROUP BY product_id
ORDER BY units_sold DESC
```

**Tabel yang dipakai:** `gold.product_daily`

**Hasil (5 teratas):**

| product_id | units_sold |
| ---------- | ---------: |
| PROD-00056 |        906 |
| PROD-00014 |        905 |
| PROD-00024 |        877 |
| PROD-00008 |        865 |
| PROD-00001 |        860 |

---

## 8. KPI harian

**Pertanyaan:** Bagaimana performa harian — order, revenue, dan refund rate?

**SQL:**

```sql
SELECT metric_date, total_orders, net_revenue, average_order_value, refund_rate
FROM gold.executive_kpis_daily
ORDER BY metric_date
```

**Tabel yang dipakai:** `gold.executive_kpis_daily`

**Hasil (5 baris pertama dari 90):**

| metric_date | total_orders | net_revenue | average_order_value | refund_rate |
| ----------- | -----------: | ----------: | ------------------: | ----------: |
| 2026-06-22  |          124 |  150.239,40 |            1.211,61 |      0,2182 |
| 2026-06-23  |          106 |  151.518,63 |            1.429,42 |      0,3333 |
| 2026-06-24  |          110 |  130.011,56 |            1.181,92 |      0,2737 |
| 2026-06-25  |           96 |  123.223,98 |            1.283,58 |      0,2151 |
| 2026-06-26  |          106 |  141.354,40 |            1.333,53 |      0,2556 |

**Penjelasan:** Rentang data 22 Juni – 19 September 2026, total 90 hari.
`refund_rate` memakai penyebut "order dengan payment captured", bukan seluruh
order — karena hanya order itulah yang benar-benar bisa direfund.
