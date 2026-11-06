DELETE FROM gold.order_360;
DELETE FROM gold.customer_daily;
DELETE FROM gold.product_daily;
DELETE FROM gold.channel_campaign_daily;
DELETE FROM gold.executive_kpis_daily;
INSERT INTO gold.order_360 (
        order_id,
        customer_id,
        order_date,
        sales_channel,
        store_id,
        gross_merchandise_value,
        discount_amount,
        shipping_revenue,
        captured_payment_amount,
        refunded_amount,
        net_revenue,
        item_count,
        unit_quantity,
        order_status,
        payment_status,
        return_status,
        promotion_count,
        first_order_flag,
        pipeline_run_id -- tidak boleh mengalami fact multiplication.
    ) WITH item_agg AS (
        -- Nilai kotor, diskon, jumlah item, dan total unit per order.
        SELECT order_id,
            COUNT(*) AS item_count,
            COALESCE(SUM(quantity), 0) AS unit_quantity,
            COALESCE(SUM(line_gross), 0) AS gross_merchandise_value,
            COALESCE(SUM(item_discount_amount), 0) AS item_discount_amount
        FROM silver.order_items
        GROUP BY order_id
    ),
    promo AS (
        -- Jumlah promosi yang menempel di tiap order.
        SELECT order_id,
            COUNT(*) AS promotion_count
        FROM silver.order_promotions
        GROUP BY order_id
    ),
    pay AS (
        -- Total pembayaran yang benar-benar tertagih.
        -- Hanya status captured/settled/paid/success yang dihitung.
        -- Split payment
        SELECT order_id,
            COALESCE(
                SUM(amount) FILTER (
                    WHERE UPPER(COALESCE(payment_status, '')) IN ('CAPTURED', 'SETTLED', 'PAID', 'SUCCESS')
                ),
                0
            ) AS captured_amount
        FROM silver.payment_events
        GROUP BY order_id
    ),
    ref AS (
        -- Total refund yang benar-benar selesai.
        -- Partial refund
        SELECT order_id,
            COALESCE(
                SUM(amount) FILTER (
                    WHERE UPPER(COALESCE(refund_status, '')) IN (
                            'COMPLETED',
                            'SETTLED',
                            'REFUNDED',
                            'CLOSED',
                            'SUCCESS'
                        )
                ),
                0
            ) AS refunded_amount
        FROM silver.refund_events
        GROUP BY order_id
    ),
    ret AS (
        -- Status retur tertinggi per order.
        -- Urutan tahap: requested(1) -> received(2) -> closed(3); ambil maksimum,
        -- karena urutan kedatangan event bisa tidak kronologis.
        SELECT order_id,
            CASE
                MAX(
                    CASE
                        return_stage
                        WHEN 'closed' THEN 3
                        WHEN 'received' THEN 2
                        WHEN 'requested' THEN 1
                        ELSE 0
                    END
                )
                WHEN 3 THEN 'closed'
                WHEN 2 THEN 'received'
                WHEN 1 THEN 'requested'
                ELSE 'none'
            END AS return_status
        FROM silver.return_events
        GROUP BY order_id
    ),
    first_order AS (
        -- Waktu order pertama per pelanggan, dipakai untuk first_order_flag.
        SELECT customer_id,
            MIN(ordered_at_utc) AS first_order_at
        FROM silver.orders
        GROUP BY customer_id
    )
SELECT o.order_id,
    o.customer_id,
    o.order_date,
    o.sales_channel,
    o.store_id,
    COALESCE(items.gross_merchandise_value, 0)::numeric(14, 2),
    COALESCE(items.item_discount_amount, 0)::numeric(14, 2),
    COALESCE(o.shipping_revenue, 0)::numeric(14, 2),
    COALESCE(pay.captured_amount, 0)::numeric(14, 2),
    COALESCE(ref.refunded_amount, 0)::numeric(14, 2),
    -- net_revenue = gross - discount + shipping - refunded  (rumus kontrak)
    (
        COALESCE(items.gross_merchandise_value, 0) - COALESCE(items.item_discount_amount, 0) + COALESCE(o.shipping_revenue, 0) - COALESCE(ref.refunded_amount, 0)
    )::numeric(14, 2),
    COALESCE(items.item_count, 0),
    COALESCE(items.unit_quantity, 0),
    o.order_status,
    -- payment_status diturunkan dari data pembayaran dan refund.
    CASE
        WHEN COALESCE(ref.refunded_amount, 0) > 0 THEN 'refunded'
        WHEN COALESCE(pay.captured_amount, 0) > 0 THEN 'captured'
        ELSE 'not_captured'
    END,
    COALESCE(ret.return_status, 'none'),
    COALESCE(promo.promotion_count, 0),
    o.ordered_at_utc = first_order.first_order_at,
    '{{pipeline_run_id}}'
FROM silver.orders o
    LEFT JOIN item_agg items ON items.order_id = o.order_id
    LEFT JOIN promo ON promo.order_id = o.order_id
    LEFT JOIN pay ON pay.order_id = o.order_id
    LEFT JOIN ref ON ref.order_id = o.order_id
    LEFT JOIN ret ON ret.order_id = o.order_id
    LEFT JOIN first_order ON first_order.customer_id = o.customer_id;
-- gold.customer_daily  —  GRAIN: satu pelanggan per tanggal bisnis
-- ---------------------------------------------------------------------------
-- Semua metric diturunkan dari gold.order_360, bukan langsung dari Silver,
-- supaya konsisten dengan grain order.
-- Tanggal yang dipakai adalah tanggal bisnis order (order_date), bukan tanggal
-- event.
-- Perhatikan channel_rank: kalau pelanggan berbelanja di dua channel pada hari
-- yang sama, ROW_NUMBER memastikan hanya SATU baris yang dihasilkan — kalau
-- tidak, grain akan dilanggar dan jumlah pelanggan jadi berlipat.
INSERT INTO gold.customer_daily (
        customer_id,
        metric_date,
        order_count,
        unit_quantity,
        gross_revenue,
        net_revenue,
        refund_amount,
        return_count,
        support_contacts,
        active_channel,
        new_customer_flag,
        repeat_customer_flag,
        pipeline_run_id
    ) WITH order_fact AS (
        -- Metric penjualan per pelanggan per hari.
        SELECT o.customer_id,
            o.order_date AS metric_date,
            COUNT(*) AS order_count,
            COALESCE(SUM(o.unit_quantity), 0) AS unit_quantity,
            COALESCE(SUM(o.gross_merchandise_value), 0) AS gross_revenue,
            COALESCE(SUM(o.net_revenue), 0) AS net_revenue,
            COALESCE(SUM(o.refunded_amount), 0) AS refund_amount
        FROM gold.order_360 o
        GROUP BY o.customer_id,
            o.order_date
    ),
    return_fact AS (
        -- Jumlah event retur yang terkait order pelanggan pada tanggal tersebut.
        SELECT o.customer_id,
            o.order_date AS metric_date,
            COUNT(*) AS return_count
        FROM silver.return_events r
            JOIN gold.order_360 o ON o.order_id = r.order_id
        GROUP BY o.customer_id,
            o.order_date
    ),
    support_fact AS (
        -- Kontak support per pelanggan per hari (memakai tanggal kejadian event).
        SELECT customer_id,
            (occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
            COUNT(*) AS support_contacts
        FROM silver.support_events
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id,
            (occurred_at_utc AT TIME ZONE 'UTC')::date
    ),
    first_seen AS (
        -- Tanggal order pertama pelanggan, untuk menandai pelanggan baru vs repeat.
        SELECT customer_id,
            MIN(order_date) AS first_date
        FROM gold.order_360
        GROUP BY customer_id
    ),
    channel_rank AS (
        -- Channel utama pelanggan pada hari itu (channel dengan order terbanyak).
        -- ROW_NUMBER menghindari duplikasi baris bila pelanggan berbelanja di >1 channel.
        SELECT customer_id,
            order_date AS metric_date,
            sales_channel,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id,
                order_date
                ORDER BY COUNT(*) DESC,
                    sales_channel
            ) AS rn
        FROM gold.order_360
        GROUP BY customer_id,
            order_date,
            sales_channel
    )
SELECT fact.customer_id,
    fact.metric_date,
    fact.order_count,
    fact.unit_quantity,
    fact.gross_revenue::numeric(14, 2),
    fact.net_revenue::numeric(14, 2),
    fact.refund_amount::numeric(14, 2),
    COALESCE(returns.return_count, 0),
    COALESCE(support.support_contacts, 0),
    channel.sales_channel,
    fact.metric_date = first_seen.first_date,
    fact.metric_date > first_seen.first_date,
    '{{pipeline_run_id}}'
FROM order_fact fact
    JOIN first_seen ON first_seen.customer_id = fact.customer_id
    LEFT JOIN return_fact returns ON returns.customer_id = fact.customer_id
    AND returns.metric_date = fact.metric_date
    LEFT JOIN support_fact support ON support.customer_id = fact.customer_id
    AND support.metric_date = fact.metric_date
    LEFT JOIN channel_rank channel ON channel.customer_id = fact.customer_id
    AND channel.metric_date = fact.metric_date
    AND channel.rn = 1;
-- ---------------------------------------------------------------------------
-- gold.product_daily  —  GRAIN: satu produk per tanggal bisnis
-- ---------------------------------------------------------------------------
-- active_category diambil dari kategori yang BERLAKU pada tanggal transaksi,
-- bukan kategori terbaru — karena product_categories adalah SCD.
-- refunded_units diestimasi proporsional (unit terjual x rasio refund order),
-- karena data refund tidak menyediakan rincian per item.
-- stockout_flag dibiarkan NULL bila tidak ada snapshot — "tidak ada data"
-- berbeda artinya dari "stok habis".
-- ---------------------------------------------------------------------------
INSERT INTO gold.product_daily (
        product_id,
        metric_date,
        active_category,
        units_sold,
        order_count,
        gross_revenue,
        net_revenue,
        refunded_units,
        available_inventory,
        reserved_inventory,
        stockout_flag,
        promotion_count,
        pipeline_run_id
    ) WITH item_lines AS (
        -- Gabungan baris item dengan konteks order (tanggal, nilai, refund).
        SELECT oi.product_id,
            o.order_date AS metric_date,
            oi.order_id,
            oi.quantity,
            oi.line_gross,
            oi.line_net,
            o.refunded_amount,
            (
                o.gross_merchandise_value - o.discount_amount + o.shipping_revenue
            ) AS pre_refund_total
        FROM silver.order_items oi
            JOIN gold.order_360 o ON o.order_id = oi.order_id
    ),
    sales AS (
        -- Penjualan per produk per hari.
        SELECT product_id,
            metric_date,
            COALESCE(SUM(quantity), 0) AS units_sold,
            COUNT(DISTINCT order_id) AS order_count,
            COALESCE(SUM(line_gross), 0) AS gross_revenue,
            COALESCE(SUM(line_net), 0) AS net_revenue
        FROM item_lines
        GROUP BY product_id,
            metric_date
    ),
    refund_units AS (
        -- Estimasi unit yang direfund: quantity x (refunded / nilai sebelum refund),
        -- dibatasi maksimum 1.0 agar tidak melebihi unit yang terjual.
        SELECT product_id,
            metric_date,
            SUM(
                quantity * LEAST(
                    1.0,
                    refunded_amount / NULLIF(pre_refund_total, 0)
                )
            ) AS refunded_units
        FROM item_lines
        WHERE refunded_amount > 0
        GROUP BY product_id,
            metric_date
    ),
    inventory AS (
        -- Stok dijumlahkan dari semua lokasi per produk per hari.
        SELECT product_id,
            snapshot_date,
            SUM(available_quantity) AS available_inventory,
            SUM(reserved_quantity) AS reserved_inventory
        FROM silver.inventory_snapshots
        GROUP BY product_id,
            snapshot_date
    ),
    category AS (
        -- Kategori berlaku per rentang tanggal (SCD).
        SELECT product_id,
            valid_from_utc::date AS valid_from,
            valid_to_utc::date AS valid_to,
            category_name
        FROM silver.product_categories
    )
SELECT sales.product_id,
    sales.metric_date,
    category.category_name,
    sales.units_sold,
    sales.order_count,
    sales.gross_revenue::numeric(14, 2),
    sales.net_revenue::numeric(14, 2),
    COALESCE(ROUND(refunds.refunded_units), 0)::integer,
    inventory.available_inventory,
    inventory.reserved_inventory,
    CASE
        WHEN inventory.available_inventory IS NULL THEN NULL
        ELSE inventory.available_inventory = 0
    END,
    -- Jumlah promosi yang aktif pada tanggal tersebut.
    -- Dihitung dari rentang berlaku promosi (silver.promotions), karena sumber
    -- order_promotions tidak menghubungkan promosi ke produk secara langsung.
    (
        SELECT COUNT(*)
        FROM silver.promotions p
        WHERE sales.metric_date BETWEEN p.start_date AND p.end_date
    ),
    '{{pipeline_run_id}}'
FROM sales
    LEFT JOIN refund_units refunds ON refunds.product_id = sales.product_id
    AND refunds.metric_date = sales.metric_date
    LEFT JOIN inventory ON inventory.product_id = sales.product_id
    AND inventory.snapshot_date = sales.metric_date
    LEFT JOIN category ON category.product_id = sales.product_id
    AND sales.metric_date BETWEEN category.valid_from AND category.valid_to;
-- gold.channel_campaign_daily  —  GRAIN: satu tanggal, channel, dan campaign
-- ---------------------------------------------------------------------------
-- Masalah yang harus dihindari: satu order TIDAK BOLEH dihitung di beberapa
-- campaign, karena akan menggandakan attributed_orders dan revenue.
--
-- Aturan atribusi yang dipakai (deterministik):
--   Untuk setiap (tanggal, channel), order diurutkan berdasarkan order_id, lalu
--   dibagi round-robin ke campaign yang berbelanja pada (tanggal, channel) itu.
--   Rumus: slot = nomor_urut mod jumlah_campaign.
-- Hasilnya: setiap order masuk tepat ke satu campaign, sehingga
--   SUM(attributed_orders) per (tanggal, channel) = jumlah order channel itu.
--
-- conversion_rate = order / sesi unik. Sekarang terisi karena events/web_events
-- sudah tersedia. Bila sesi tidak ada, kolomnya NULL — bukan angka palsu.
INSERT INTO gold.channel_campaign_daily (
        metric_date,
        sales_channel,
        campaign_id,
        campaign_spend,
        attributed_orders,
        attributed_customers,
        gross_revenue,
        net_revenue,
        refunds,
        roas,
        conversion_rate,
        pipeline_run_id
    ) WITH campaign_slots AS (
        -- Daftar campaign per (tanggal, channel) beserta nomor slot 0..n-1.
        SELECT spend_date,
            channel,
            campaign_id,
            ROW_NUMBER() OVER (
                PARTITION BY spend_date,
                channel
                ORDER BY campaign_id
            ) - 1 AS slot,
            COUNT(*) OVER (PARTITION BY spend_date, channel) AS slot_count
        FROM silver.campaign_spend
    ),
    ordered AS (
        -- Nomor urut order per (tanggal, channel), dipakai untuk round-robin.
        SELECT order_id,
            order_date,
            sales_channel,
            customer_id,
            gross_merchandise_value,
            net_revenue,
            refunded_amount,
            ROW_NUMBER() OVER (
                PARTITION BY order_date,
                sales_channel
                ORDER BY order_id
            ) - 1 AS rn
        FROM gold.order_360
    ),
    attributed AS (
        -- Setiap order dipasangkan ke tepat satu campaign lewat rumus modulo.
        SELECT ordered.order_date AS metric_date,
            ordered.sales_channel,
            slots.campaign_id,
            ordered.order_id,
            ordered.customer_id,
            ordered.gross_merchandise_value,
            ordered.net_revenue,
            ordered.refunded_amount
        FROM ordered
            JOIN campaign_slots slots ON slots.spend_date = ordered.order_date
            AND slots.channel = ordered.sales_channel
            AND slots.slot = ordered.rn % slots.slot_count
    ),
    attributed_agg AS (
        -- Agregasi hasil atribusi ke grain (tanggal, channel, campaign).
        SELECT metric_date,
            sales_channel,
            campaign_id,
            COUNT(DISTINCT order_id) AS attributed_orders,
            COUNT(DISTINCT customer_id) AS attributed_customers,
            COALESCE(SUM(gross_merchandise_value), 0) AS gross_revenue,
            COALESCE(SUM(net_revenue), 0) AS net_revenue,
            COALESCE(SUM(refunded_amount), 0) AS refunds
        FROM attributed
        GROUP BY metric_date,
            sales_channel,
            campaign_id
    ),
    sessions AS (
        -- Jumlah sesi unik per (tanggal, channel), dasar conversion_rate.
        SELECT (occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
            channel AS sales_channel,
            COUNT(DISTINCT session_id) AS session_count
        FROM silver.web_events
        WHERE session_id IS NOT NULL
            AND channel IS NOT NULL
        GROUP BY 1,
            2
    )
SELECT spend.spend_date,
    spend.channel,
    spend.campaign_id,
    spend.spend_amount::numeric(14, 2),
    COALESCE(agg.attributed_orders, 0),
    COALESCE(agg.attributed_customers, 0),
    COALESCE(agg.gross_revenue, 0)::numeric(14, 2),
    COALESCE(agg.net_revenue, 0)::numeric(14, 2),
    COALESCE(agg.refunds, 0)::numeric(14, 2),
    -- ROAS = attributed net revenue / campaign spend.
    -- NULL bila spend 0, supaya tidak terjadi pembagian nol.
    CASE
        WHEN spend.spend_amount > 0 THEN (
            COALESCE(agg.net_revenue, 0) / spend.spend_amount
        )::numeric(14, 4)
        ELSE NULL
    END,
    -- conversion_rate = order / sesi unik. NULL bila data sesi tidak tersedia.
    CASE
        WHEN sessions.session_count IS NULL
        OR sessions.session_count = 0 THEN NULL
        ELSE (
            COALESCE(agg.attributed_orders, 0)::numeric / sessions.session_count
        )::numeric(14, 4)
    END,
    '{{pipeline_run_id}}'
FROM silver.campaign_spend spend
    LEFT JOIN attributed_agg agg ON agg.metric_date = spend.spend_date
    AND agg.sales_channel = spend.channel
    AND agg.campaign_id = spend.campaign_id
    LEFT JOIN sessions ON sessions.metric_date = spend.spend_date
    AND sessions.sales_channel = spend.channel;
-- gold.executive_kpis_daily  —  GRAIN: satu tanggal bisnis
-- ---------------------------------------------------------------------------
-- Definisi metric sesuai kontrak:
--   average_order_value  = net_revenue / total_orders
--   refund_rate          = order yang punya refund / order dengan payment captured
--   return_rate          = order yang punya return / resolved order
--   repeat_customer_rate = pelanggan repeat / baris pelanggan aktif hari itu
--   stockout_rate        = produk stockout / produk yang punya data inventory
--   support_contact_rate = pelanggan yang menghubungi support / pelanggan aktif
--
-- Perhatikan penyebut tiap rate — bukan sekadar "jumlah order", tapi populasi
-- yang memang relevan. refund_rate memakai order yang payment-nya captured,
-- karena hanya order itulah yang benar-benar bisa direfund.
INSERT INTO gold.executive_kpis_daily (
        metric_date,
        total_orders,
        gross_revenue,
        net_revenue,
        average_order_value,
        refund_rate,
        return_rate,
        repeat_customer_rate,
        stockout_rate,
        active_customers,
        support_contact_rate,
        data_freshness_utc,
        pipeline_run_id
    ) WITH order_fact AS (
        -- Metric tingkat order per tanggal, termasuk pembilang/penyebut tiap rate.
        SELECT order_date AS metric_date,
            COUNT(*) AS total_orders,
            COALESCE(SUM(gross_merchandise_value), 0) AS gross_revenue,
            COALESCE(SUM(net_revenue), 0) AS net_revenue,
            COUNT(*) FILTER (
                WHERE captured_payment_amount > 0
            ) AS captured_orders,
            COUNT(*) FILTER (
                WHERE refunded_amount > 0
            ) AS refunded_orders,
            COUNT(*) FILTER (
                WHERE return_status <> 'none'
            ) AS returned_orders,
            COUNT(*) FILTER (
                WHERE order_status IN ('fulfilled', 'returned')
            ) AS resolved_orders,
            COUNT(DISTINCT customer_id) AS active_customers
        FROM gold.order_360
        GROUP BY order_date
    ),
    customer_fact AS (
        -- Pelanggan repeat per tanggal (dari customer_daily).
        SELECT metric_date,
            COUNT(*) FILTER (
                WHERE repeat_customer_flag
            ) AS repeat_customers,
            COUNT(*) AS customer_rows
        FROM gold.customer_daily
        GROUP BY metric_date
    ),
    support_fact AS (
        -- Pelanggan unik yang menghubungi support per tanggal.
        SELECT (occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
            COUNT(DISTINCT customer_id) AS support_customers
        FROM silver.support_events
        WHERE customer_id IS NOT NULL
        GROUP BY 1
    ),
    product_fact AS (
        -- Produk stockout vs produk yang punya data inventory.
        SELECT metric_date,
            COUNT(*) FILTER (
                WHERE stockout_flag IS TRUE
            ) AS stockout_products,
            COUNT(*) FILTER (
                WHERE stockout_flag IS NOT NULL
            ) AS products_with_inventory
        FROM gold.product_daily
        GROUP BY metric_date
    )
SELECT order_fact.metric_date,
    order_fact.total_orders,
    order_fact.gross_revenue::numeric(14, 2),
    order_fact.net_revenue::numeric(14, 2),
    -- AOV: 0 bila tidak ada order (menghindari pembagian nol)
    CASE
        WHEN order_fact.total_orders > 0 THEN (order_fact.net_revenue / order_fact.total_orders)::numeric(14, 2)
        ELSE 0::numeric(14, 2)
    END,
    -- refund_rate: dibandingkan dengan order yang payment-nya captured
    CASE
        WHEN order_fact.captured_orders > 0 THEN (
            order_fact.refunded_orders::numeric / order_fact.captured_orders
        )::numeric(14, 4)
        ELSE 0::numeric(14, 4)
    END,
    -- return_rate: dibandingkan dengan resolved order
    CASE
        WHEN order_fact.resolved_orders > 0 THEN (
            order_fact.returned_orders::numeric / order_fact.resolved_orders
        )::numeric(14, 4)
        ELSE 0::numeric(14, 4)
    END,
    -- repeat_customer_rate
    CASE
        WHEN COALESCE(customer_fact.customer_rows, 0) > 0 THEN (
            customer_fact.repeat_customers::numeric / customer_fact.customer_rows
        )::numeric(14, 4)
        ELSE 0::numeric(14, 4)
    END,
    -- stockout_rate
    CASE
        WHEN COALESCE(product_fact.products_with_inventory, 0) > 0 THEN (
            product_fact.stockout_products::numeric / product_fact.products_with_inventory
        )::numeric(14, 4)
        ELSE 0::numeric(14, 4)
    END,
    order_fact.active_customers,
    -- support_contact_rate
    CASE
        WHEN order_fact.active_customers > 0 THEN (
            COALESCE(support_fact.support_customers, 0)::numeric / order_fact.active_customers
        )::numeric(14, 4)
        ELSE 0::numeric(14, 4)
    END,
    now(),
    '{{pipeline_run_id}}'
FROM order_fact
    LEFT JOIN customer_fact ON customer_fact.metric_date = order_fact.metric_date
    LEFT JOIN support_fact ON support_fact.metric_date = order_fact.metric_date
    LEFT JOIN product_fact ON product_fact.metric_date = order_fact.metric_date;