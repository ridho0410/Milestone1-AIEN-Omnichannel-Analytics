-- Tugas layer Silver: mengubah payload JSONB mentah dari Bronze menjadi tabel
-- bertipe dan ter-normalisasi.
-- Yang sudah dikerjakan di bagian ini:
--   - waktu dinormalkan ke UTC
--   - duplikat dibuang secara pasti (deterministik)
--   - riwayat versi untuk profil pelanggan, alamat, dan kategori produk
--   - penanda asal data (lineage) ke Bronze
DELETE FROM silver.rejected_records;
DELETE FROM silver.order_promotions;
DELETE FROM silver.payment_events;
DELETE FROM silver.refund_events;
DELETE FROM silver.return_events;
DELETE FROM silver.support_events;
DELETE FROM silver.web_events;
DELETE FROM silver.inventory_snapshots;
DELETE FROM silver.campaign_spend;
DELETE FROM silver.order_items;
DELETE FROM silver.orders;
DELETE FROM silver.promotions;
DELETE FROM silver.city_reference;
DELETE FROM silver.sales_channels;
DELETE FROM silver.stores;
DELETE FROM silver.product_categories;
DELETE FROM silver.products;
DELETE FROM silver.customer_addresses;
DELETE FROM silver.customer_profiles;
DELETE FROM silver.customers;
-- MASTER PELANGGAN
-- Sumber: operational/customers.json. Satu baris per pelanggan.
-- Bila ada customer_id yang muncul lebih dari sekali, hanya satu yang diambil
-- — dipilih menurut urutan yang pasti, jadi hasilnya selalu sama di tiap run.
-- Waktu pendaftaran disimpan sebagai timestamptz.
INSERT INTO silver.customers (
        customer_id,
        city_id,
        customer_segment,
        created_at_utc,
        source_row_id,
        pipeline_run_id
    )
SELECT DISTINCT ON (payload->>'customer_id') payload->>'customer_id',
    payload->>'city_id',
    payload->>'customer_segment',
    (payload->>'created_at_utc')::timestamptz,
    payload->>'source_row_id',
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/customers.json'
    AND payload->>'customer_id' IS NOT NULL
ORDER BY payload->>'customer_id',
    payload->>'source_row_id';
-- RIWAYAT PROFIL PELANGGAN
-- Sumber: operational/customer_profiles.json.
-- Berbeda dari tabel di atas, tabel ini menyimpan SETIAP versi profil beserta
-- rentang waktu berlakunya. Satu pelanggan bisa punya beberapa baris.
-- Kolom is_current menandai versi yang paling akhir. Nanti Gold memakainya
-- untuk memilih versi yang berlaku pada tanggal sebuah order dibuat.
INSERT INTO silver.customer_profiles (
        customer_id,
        valid_from_utc,
        valid_to_utc,
        city_id,
        customer_segment,
        source_row_id,
        is_current,
        pipeline_run_id
    )
SELECT payload->>'customer_id',
    (payload->>'valid_from_utc')::timestamptz,
    (payload->>'valid_to_utc')::timestamptz,
    payload->>'city_id',
    payload->>'customer_segment',
    payload->>'source_row_id',
    (payload->>'valid_to_utc')::timestamptz >= (
        SELECT MAX((payload->>'valid_to_utc')::timestamptz)
        FROM bronze.raw_records
        WHERE source_file = 'operational/customer_profiles.json'
    ),
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/customer_profiles.json'
    AND payload->>'customer_id' IS NOT NULL
    AND payload->>'valid_from_utc' IS NOT NULL;
-- RIWAYAT ALAMAT PELANGGAN
-- Sumber: operational/customer_addresses.json.
-- Strukturnya sama seperti riwayat profil: menyimpan versi beserta rentang
-- berlakunya, supaya alamat yang berlaku pada saat transaksi bisa dipilih.
INSERT INTO silver.customer_addresses (
        address_id,
        customer_id,
        city_id,
        valid_from_utc,
        valid_to_utc,
        is_current,
        source_row_id,
        pipeline_run_id
    )
SELECT payload->>'address_id',
    payload->>'customer_id',
    payload->>'city_id',
    (payload->>'valid_from_utc')::timestamptz,
    (payload->>'valid_to_utc')::timestamptz,
    (payload->>'valid_to_utc')::timestamptz >= (
        SELECT MAX((payload->>'valid_to_utc')::timestamptz)
        FROM bronze.raw_records
        WHERE source_file = 'operational/customer_addresses.json'
    ),
    payload->>'source_row_id',
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/customer_addresses.json'
    AND payload->>'address_id' IS NOT NULL;
-- MASTER PRODUK
-- Sumber: operational/products.json. Satu baris per produk.
-- Harga disimpan sebagai NUMERIC (desimal), bukan bilangan pecahan komputer,
-- supaya perhitungan uang tidak menimbulkan galat pembulatan.
INSERT INTO silver.products (
        product_id,
        product_name,
        sku,
        unit_price,
        source_row_id,
        pipeline_run_id
    )
SELECT DISTINCT ON (payload->>'product_id') payload->>'product_id',
    payload->>'product_name',
    payload->>'sku',
    (payload->>'unit_price')::numeric(14, 2),
    payload->>'source_row_id',
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/products.json'
    AND payload->>'product_id' IS NOT NULL
ORDER BY payload->>'product_id',
    payload->>'source_row_id';
-- RIWAYAT KATEGORI PRODUK
-- Sumber: operational/product_categories.json.
-- Produk bisa berpindah kategori, jadi tiap kategori disimpan bersama rentang
-- waktu berlakunya. Dengan begitu laporan "penjualan per kategori" memakai
-- kategori yang benar pada saat penjualan terjadi, bukan kategori terbaru.
INSERT INTO silver.product_categories (
        product_id,
        valid_from_utc,
        valid_to_utc,
        category_id,
        category_name,
        source_row_id,
        is_current,
        pipeline_run_id
    )
SELECT payload->>'product_id',
    (payload->>'valid_from_utc')::timestamptz,
    (payload->>'valid_to_utc')::timestamptz,
    payload->>'category_id',
    payload->>'category_name',
    payload->>'source_row_id',
    (payload->>'valid_to_utc')::timestamptz >= (
        SELECT MAX((payload->>'valid_to_utc')::timestamptz)
        FROM bronze.raw_records
        WHERE source_file = 'operational/product_categories.json'
    ),
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/product_categories.json'
    AND payload->>'product_id' IS NOT NULL
    AND payload->>'valid_from_utc' IS NOT NULL;
-- MASTER TOKO
-- Sumber: operational/stores.json. Daftar toko fisik milik perusahaan.
INSERT INTO silver.stores (
        store_id,
        store_name,
        city_id,
        location_type,
        source_row_id,
        pipeline_run_id
    )
SELECT DISTINCT ON (payload->>'store_id') payload->>'store_id',
    payload->>'store_name',
    payload->>'city_id',
    payload->>'location_type',
    payload->>'source_row_id',
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/stores.json'
    AND payload->>'store_id' IS NOT NULL
ORDER BY payload->>'store_id',
    payload->>'source_row_id';
-- MASTER CHANNEL PENJUALAN
-- Sumber: operational/sales_channels.json.
-- Berisi empat channel: WEB, MOBILE_APP, MARKETPLACE, STORE.
INSERT INTO silver.sales_channels (
        channel_id,
        channel_name,
        source_row_id,
        pipeline_run_id
    )
SELECT DISTINCT ON (payload->>'channel_id') payload->>'channel_id',
    payload->>'channel_name',
    payload->>'source_row_id',
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/sales_channels.json'
    AND payload->>'channel_id' IS NOT NULL
ORDER BY payload->>'channel_id',
    payload->>'source_row_id';
-- KAMUS KOTA
-- Sumber: reference/city_reference.json.
-- Menerjemahkan kode kota (mis. JKT) menjadi nama kota dan negara.
INSERT INTO silver.city_reference (city_id, city_name, country, pipeline_run_id)
SELECT DISTINCT ON (payload->>'city_id') payload->>'city_id',
    payload->>'city_name',
    payload->>'country',
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'reference/city_reference.json'
    AND payload->>'city_id' IS NOT NULL
ORDER BY payload->>'city_id';
-- MASTER PROMOSI
-- Sumber: operational/promotions.json.
-- Menyimpan setiap program promosi beserta periode berlakunya, sehingga bisa
-- dihitung berapa promosi yang aktif pada tanggal tertentu.
INSERT INTO silver.promotions (
        promotion_id,
        promotion_code,
        promotion_type,
        discount_rate,
        start_date,
        end_date,
        source_row_id,
        pipeline_run_id
    )
SELECT DISTINCT ON (payload->>'promotion_id') payload->>'promotion_id',
    payload->>'promotion_code',
    payload->>'promotion_type',
    (payload->>'discount_rate')::numeric(14, 4),
    (payload->>'start_date')::date,
    (payload->>'end_date')::date,
    payload->>'source_row_id',
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/promotions.json'
    AND payload->>'promotion_id' IS NOT NULL
ORDER BY payload->>'promotion_id',
    payload->>'source_row_id';
-- ORDER
-- Sumber: operational/orders.json.
-- Berisi 10.001 baris untuk 10.000 order — ada satu order yang tercatat dua
-- kali. Aturannya: dari order yang sama diambil versi dengan waktu update
-- paling baru; bila seri, nomor baris terbesar. Aturannya pasti, jadi hasilnya
-- selalu sama di tiap run — bukan bergantung urutan file.
-- Kolom had_duplicate menandai order yang tadinya duplikat, supaya jejaknya
-- tetap ada dan bisa diaudit.
-- Tanggal bisnis dihitung dalam UTC secara eksplisit, tidak mengikuti setting
WITH ranked AS (
    SELECT payload,
        source_row_number,
        ROW_NUMBER() OVER (
            PARTITION BY payload->>'order_id'
            ORDER BY (payload->>'updated_at_utc')::timestamptz DESC NULLS LAST,
                source_row_number DESC --Duplicate order
        ) AS rn,
        COUNT(*) OVER (PARTITION BY payload->>'order_id') AS duplicate_count
    FROM bronze.raw_records
    WHERE source_file = 'operational/orders.json'
        AND payload->>'order_id' IS NOT NULL
)
INSERT INTO silver.orders (
        order_id,
        customer_id,
        ordered_at_utc,
        order_date,
        sales_channel,
        store_id,
        status_raw,
        order_status,
        shipping_revenue,
        updated_at_utc,
        source_row_id,
        had_duplicate,
        pipeline_run_id
    )
SELECT payload->>'order_id',
    payload->>'customer_id',
    (payload->>'ordered_at_utc')::timestamptz,
    (
        (payload->>'ordered_at_utc')::timestamptz AT TIME ZONE 'UTC'
    )::date,
    payload->>'sales_channel',
    NULLIF(payload->>'store_id', ''),
    payload->>'status',
    CASE
        UPPER(COALESCE(payload->>'status', 'UNKNOWN'))
        WHEN 'PLACED' THEN 'placed'
        WHEN 'CONFIRMED' THEN 'confirmed'
        WHEN 'FULFILLED' THEN 'fulfilled'
        WHEN 'CANCELLED' THEN 'cancelled'
        WHEN 'RETURNED' THEN 'returned'
        ELSE LOWER(COALESCE(payload->>'status', 'unknown'))
    END,
    COALESCE(
        (payload->>'shipping_revenue')::numeric(14, 2),
        0
    ),
    (payload->>'updated_at_utc')::timestamptz,
    payload->>'source_row_id',
    duplicate_count > 1,
    '{{pipeline_run_id}}'
FROM ranked
WHERE rn = 1;
-- BARIS ITEM ORDER
-- Sumber: operational/order_items.json.
-- Hanya baris yang lolos TIGA syarat yang masuk ke sini:
--   1. quantity lebih besar dari 0
--   2. order_id-nya ada di tabel orders
--   3. product_id-nya ada di tabel products
-- Baris yang tidak lolos tidak dihapus diam-diam — ia dicatat di
-- silver.rejected_records beserta alasannya (lihat blok berikutnya).
-- line_gross dan line_net dihitung di sini supaya Gold tidak perlu mengulang
-- perkalian yang sama.
INSERT INTO silver.order_items (
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        item_discount_amount,
        line_gross,
        line_net,
        source_row_id,
        pipeline_run_id
    )
SELECT payload->>'order_item_id',
    payload->>'order_id',
    payload->>'product_id',
    (payload->>'quantity')::integer,
    COALESCE((payload->>'unit_price')::numeric(14, 2), 0),
    COALESCE(
        (payload->>'item_discount_amount')::numeric(14, 2),
        0
    ),
    COALESCE((payload->>'quantity')::numeric, 0) * COALESCE((payload->>'unit_price')::numeric, 0),
    COALESCE((payload->>'quantity')::numeric, 0) * COALESCE((payload->>'unit_price')::numeric, 0) - COALESCE((payload->>'item_discount_amount')::numeric, 0),
    payload->>'source_row_id',
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/order_items.json'
    AND payload->>'order_item_id' IS NOT NULL
    AND COALESCE((payload->>'quantity')::numeric, 0) > 0
    AND payload->>'order_id' IN (
        SELECT order_id
        FROM silver.orders
    )
    AND payload->>'product_id' IN (
        SELECT product_id
        FROM silver.products
    );
-- RECORD YANG DITOLAK — tiga alasan untuk baris item order
-- Setiap baris menyimpan payload aslinya, sehingga penolakan bisa diaudit.
-- Tidak ada baris yang hilang tanpa jejak.
-- Alasan 1: quantity nol atau negatif
INSERT INTO silver.rejected_records (
        pipeline_run_id,
        source_file,
        source_row_number,
        source_record_id,
        reason_code,
        reason_detail,
        raw_payload
    )
SELECT '{{pipeline_run_id}}',
    source_file,
    source_row_number,
    source_record_id,
    'NEGATIVE_OR_ZERO_QUANTITY',
    'quantity <= 0',
    payload
FROM bronze.raw_records
WHERE source_file = 'operational/order_items.json'
    AND COALESCE((payload->>'quantity')::numeric, 0) <= 0;
-- Alasan 2: produk tidak dikenal
INSERT INTO silver.rejected_records (
        pipeline_run_id,
        source_file,
        source_row_number,
        source_record_id,
        reason_code,
        reason_detail,
        raw_payload
    )
SELECT '{{pipeline_run_id}}',
    source_file,
    source_row_number,
    source_record_id,
    'BROKEN_PRODUCT_REFERENCE',
    'product_id tidak ditemukan di silver.products',
    payload
FROM bronze.raw_records
WHERE source_file = 'operational/order_items.json'
    AND COALESCE((payload->>'quantity')::numeric, 0) > 0
    AND payload->>'product_id' NOT IN (
        SELECT product_id
        FROM silver.products
    );
-- Alasan 3: order tidak dikenal
INSERT INTO silver.rejected_records (
        pipeline_run_id,
        source_file,
        source_row_number,
        source_record_id,
        reason_code,
        reason_detail,
        raw_payload
    )
SELECT '{{pipeline_run_id}}',
    source_file,
    source_row_number,
    source_record_id,
    'BROKEN_ORDER_REFERENCE',
    'order_id tidak ditemukan di silver.orders',
    payload
FROM bronze.raw_records
WHERE source_file = 'operational/order_items.json'
    AND COALESCE((payload->>'quantity')::numeric, 0) > 0
    AND payload->>'order_id' NOT IN (
        SELECT order_id
        FROM silver.orders
    );
-- EVENT PEMBAYARAN
-- Sumber: events/payment_events.json.
-- Event yang sama bisa terkirim lebih dari sekali, jadi hanya satu baris yang
-- diambil per event_id — dipilih yang paling terakhir diterima sistem.
-- Status pembayaran diambil dari tipe event (PAYMENT_CAPTURED -> CAPTURED),
-- karena file sumber tidak menyimpan kolom status tersendiri.
-- Kolom is_late menandai event yang baru diterima lebih dari 24 jam setelah
-- kejadiannya. Event terlambat tetap dipakai, hanya ditandai — bukan dibuang.
-- Payload aslinya disimpan di raw_payload supaya masih bisa diaudit.
INSERT INTO silver.payment_events (
        event_id,
        order_id,
        event_type,
        payment_status,
        amount,
        occurred_at_utc,
        ingested_at_utc,
        is_late,
        raw_payload,
        pipeline_run_id
    ) --Duplicate event    
SELECT DISTINCT ON (payload->>'event_id') payload->>'event_id',
    COALESCE(
        payload->>'order_id',
        payload#>>'{payload,order_id}'
    ),
    COALESCE(
        payload->>'event_type',
        payload#>>'{payload,event_type}'
    ),
    COALESCE(
        payload->>'payment_status',
        payload->>'status',
        UPPER(
            REPLACE(
                COALESCE(payload->>'event_type', ''),
                'PAYMENT_',
                ''
            )
        )
    ),
    COALESCE(
        (payload->>'amount')::numeric(14, 2),
        (payload#>>'{payload,amount}')::numeric(14, 2)
    ),
    COALESCE(
        (payload->>'occurred_at_utc')::timestamptz,
        (payload#>>'{payload,occurred_at_utc}')::timestamptz
    ),
    COALESCE(
        (payload->>'ingested_at_utc')::timestamptz,
        (payload#>>'{payload,ingested_at_utc}')::timestamptz
    ),
    -- Event terlambat dan timestamp tidak berurutan
    COALESCE(
        (payload->>'ingested_at_utc')::timestamptz,
        (payload#>>'{payload,ingested_at_utc}')::timestamptz
    ) - COALESCE(
        (payload->>'occurred_at_utc')::timestamptz,
        (payload#>>'{payload,occurred_at_utc}')::timestamptz
    ) > INTERVAL '24 hours',
    payload,
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'events/payment_events.json'
    AND payload->>'event_id' IS NOT NULL
ORDER BY payload->>'event_id',
    (payload->>'ingested_at_utc')::timestamptz DESC NULLS LAST,
    source_row_number DESC;
-- EVENT REFUND
-- Sumber: events/refund_events.json.
-- Aturannya sama seperti event pembayaran: satu baris per event_id, status
-- diambil dari tipe event (REFUND_COMPLETED -> COMPLETED), dan event yang
-- datang terlambat ditandai is_late.
INSERT INTO silver.refund_events (
        event_id,
        order_id,
        event_type,
        refund_status,
        amount,
        occurred_at_utc,
        ingested_at_utc,
        is_late,
        raw_payload,
        pipeline_run_id
    ) --Duplicate event
SELECT DISTINCT ON (payload->>'event_id') payload->>'event_id',
    COALESCE(
        payload->>'order_id',
        payload#>>'{payload,order_id}'
    ),
    COALESCE(
        payload->>'event_type',
        payload#>>'{payload,event_type}'
    ),
    COALESCE(
        payload->>'refund_status',
        payload->>'status',
        UPPER(
            REPLACE(
                COALESCE(payload->>'event_type', ''),
                'REFUND_',
                ''
            )
        )
    ),
    COALESCE(
        (payload->>'amount')::numeric(14, 2),
        (payload#>>'{payload,amount}')::numeric(14, 2)
    ),
    COALESCE(
        (payload->>'occurred_at_utc')::timestamptz,
        (payload#>>'{payload,occurred_at_utc}')::timestamptz
    ),
    COALESCE(
        (payload->>'ingested_at_utc')::timestamptz,
        (payload#>>'{payload,ingested_at_utc}')::timestamptz
    ),
    COALESCE(
        (payload->>'ingested_at_utc')::timestamptz,
        (payload#>>'{payload,ingested_at_utc}')::timestamptz
    ) - COALESCE(
        (payload->>'occurred_at_utc')::timestamptz,
        (payload#>>'{payload,occurred_at_utc}')::timestamptz
    ) > INTERVAL '24 hours',
    payload,
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'events/refund_events.json'
    AND payload->>'event_id' IS NOT NULL
ORDER BY payload->>'event_id',
    (payload->>'ingested_at_utc')::timestamptz DESC NULLS LAST,
    source_row_number DESC;
-- EVENT RETUR
-- Sumber: events/return_events.json.
-- Satu retur menghasilkan beberapa event berurutan: diajukan, barang diterima,
-- lalu ditutup. Tahapnya diringkas menjadi requested / received / closed.
-- Nanti Gold mengambil tahap TERTINGGI per order, karena urutan kedatangan
-- event bisa tidak kronologis.
INSERT INTO silver.return_events (
        event_id,
        order_id,
        return_id,
        event_type,
        return_stage,
        occurred_at_utc,
        ingested_at_utc,
        is_late,
        raw_payload,
        pipeline_run_id
    ) --Duplicate event
SELECT DISTINCT ON (payload->>'event_id') payload->>'event_id',
    payload#>>'{payload,order_id}',
    payload#>>'{payload,return_id}',
    payload->>'event_type',
    CASE
        WHEN payload->>'event_type' LIKE '%REQUESTED%' THEN 'requested'
        WHEN payload->>'event_type' LIKE '%RECEIVED%' THEN 'received'
        WHEN payload->>'event_type' LIKE '%CLOSED%' THEN 'closed'
        ELSE LOWER(COALESCE(payload->>'event_type', 'unknown'))
    END,
    (payload->>'occurred_at_utc')::timestamptz,
    (payload->>'ingested_at_utc')::timestamptz,
    (payload->>'ingested_at_utc')::timestamptz - (payload->>'occurred_at_utc')::timestamptz > INTERVAL '24 hours',
    payload,
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'events/return_events.json'
    AND payload->>'event_id' IS NOT NULL
ORDER BY payload->>'event_id',
    (payload->>'ingested_at_utc')::timestamptz DESC NULLS LAST,
    source_row_number DESC;
-- EVENT CUSTOMER SUPPORT
-- Sumber: events/support_events.json.
-- Tiket support punya dua tahap: dibuat lalu ditutup, diringkas menjadi
-- created / closed. Kolom reason berisi alasan pelanggan menghubungi.
INSERT INTO silver.support_events (
        event_id,
        ticket_id,
        customer_id,
        order_id,
        reason,
        event_type,
        ticket_stage,
        occurred_at_utc,
        ingested_at_utc,
        is_late,
        raw_payload,
        pipeline_run_id
    ) --Duplicate event
SELECT DISTINCT ON (payload->>'event_id') payload->>'event_id',
    payload#>>'{payload,ticket_id}',
    payload#>>'{payload,customer_id}',
    payload#>>'{payload,order_id}',
    payload#>>'{payload,reason}',
    payload->>'event_type',
    CASE
        WHEN payload->>'event_type' LIKE '%CREATED%' THEN 'created'
        WHEN payload->>'event_type' LIKE '%CLOSED%' THEN 'closed'
        ELSE LOWER(COALESCE(payload->>'event_type', 'unknown'))
    END,
    (payload->>'occurred_at_utc')::timestamptz,
    (payload->>'ingested_at_utc')::timestamptz,
    (payload->>'ingested_at_utc')::timestamptz - (payload->>'occurred_at_utc')::timestamptz > INTERVAL '24 hours',
    payload,
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'events/support_events.json'
    AND payload->>'event_id' IS NOT NULL
ORDER BY payload->>'event_id',
    (payload->>'ingested_at_utc')::timestamptz DESC NULLS LAST,
    source_row_number DESC;
-- EVENT WEB / MOBILE
-- Sumber: events/web_events.json.
-- Dipakai hanya untuk menghitung conversion rate (jumlah sesi unik per
-- tanggal dan channel). event_id yang muncul berkali-kali tetap dibuang.
INSERT INTO silver.web_events (
        event_id,
        session_id,
        customer_id,
        channel,
        event_type,
        occurred_at_utc,
        ingested_at_utc,
        is_late,
        raw_payload,
        pipeline_run_id
    ) --Duplicate event
SELECT DISTINCT ON (payload->>'event_id') payload->>'event_id',
    COALESCE(
        payload->>'session_id',
        payload#>>'{payload,session_id}'
    ),
    COALESCE(
        payload->>'customer_id',
        payload#>>'{payload,customer_id}'
    ),
    COALESCE(
        payload->>'channel',
        payload->>'sales_channel',
        payload#>>'{payload,channel}'
    ),
    COALESCE(
        payload->>'event_type',
        payload#>>'{payload,event_type}'
    ),
    COALESCE(
        (payload->>'occurred_at_utc')::timestamptz,
        (payload#>>'{payload,occurred_at_utc}')::timestamptz
    ),
    COALESCE(
        (payload->>'ingested_at_utc')::timestamptz,
        (payload#>>'{payload,ingested_at_utc}')::timestamptz
    ),
    COALESCE(
        (payload->>'ingested_at_utc')::timestamptz,
        (payload#>>'{payload,ingested_at_utc}')::timestamptz
    ) - COALESCE(
        (payload->>'occurred_at_utc')::timestamptz,
        (payload#>>'{payload,occurred_at_utc}')::timestamptz
    ) > INTERVAL '24 hours',
    payload,
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'events/web_events.json'
    AND payload->>'event_id' IS NOT NULL
ORDER BY payload->>'event_id',
    (payload->>'ingested_at_utc')::timestamptz DESC NULLS LAST,
    source_row_number DESC;
-- SNAPSHOT STOK HARIAN
-- Sumber: inventory/inventory_snapshots.csv.
-- File CSV menyimpan semua nilai sebagai teks, jadi tipe sebenarnya di-cast
-- di sini: tanggal, bilangan bulat, dan biaya satuan.
INSERT INTO silver.inventory_snapshots (
        product_id,
        location_id,
        snapshot_date,
        available_quantity,
        reserved_quantity,
        unit_cost,
        pipeline_run_id
    )
SELECT payload->>'product_id',
    payload->>'location_id',
    (payload->>'snapshot_date')::date,
    (payload->>'available_quantity')::integer,
    (payload->>'reserved_quantity')::integer,
    (payload->>'unit_cost')::numeric(14, 2),
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'inventory/inventory_snapshots.csv'
    AND payload->>'product_id' IS NOT NULL
    AND payload->>'snapshot_date' IS NOT NULL;
-- BELANJA CAMPAIGN
-- Sumber: reference/campaign_spend.csv.
-- Berapa uang yang dibelanjakan untuk iklan per tanggal, campaign, dan channel.
-- Jadi bahan utama ROAS di Gold.
INSERT INTO silver.campaign_spend (
        spend_date,
        campaign_id,
        channel,
        spend_amount,
        pipeline_run_id
    )
SELECT (payload->>'spend_date')::date,
    payload->>'campaign_id',
    payload->>'channel',
    (payload->>'spend_amount')::numeric(14, 2),
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'reference/campaign_spend.csv'
    AND payload->>'campaign_id' IS NOT NULL
    AND payload->>'spend_date' IS NOT NULL;
-- JEMBATAN ORDER <-> PROMOSI
-- Sumber: operational/order_promotions.json.
-- Menghubungkan order dengan promosi yang dipakainya.
-- Dua penyesuaian dari kode acuan, karena file sumber berbeda:
--   1. File sumber memakai nama kolom discount_amount, bukan applied_discount
--   2. File sumber tidak punya promotion_code -> diambil dari tabel promotions
INSERT INTO silver.order_promotions (
        order_id,
        promotion_id,
        promotion_code,
        applied_discount,
        pipeline_run_id
    )
SELECT payload->>'order_id',
    payload->>'promotion_id',
    COALESCE(
        payload->>'promotion_code',
        (
            SELECT p.promotion_code
            FROM silver.promotions p
            WHERE p.promotion_id = payload->>'promotion_id'
        )
    ),
    COALESCE(
        (payload->>'applied_discount')::numeric(14, 2),
        (payload->>'discount_amount')::numeric(14, 2)
    ),
    '{{pipeline_run_id}}'
FROM bronze.raw_records
WHERE source_file = 'operational/order_promotions.json'
    AND payload->>'order_id' IS NOT NULL
    AND payload->>'promotion_id' IS NOT NULL;
-- RECORD YANG DITOLAK — alasan keempat: event duplikat
-- Event dengan event_id yang sama dibuang; yang dipertahankan adalah yang
-- paling terakhir diterima sistem. Baris yang dibuang tetap DICATAT di sini,
-- sehingga deduplikasi bisa diaudit — bukan penghapusan diam-diam.
INSERT INTO silver.rejected_records (
        pipeline_run_id,
        source_file,
        source_row_number,
        source_record_id,
        reason_code,
        reason_detail,
        raw_payload
    ) --Duplicate event = DUPLICATE_EVENT_DROPPED
SELECT '{{pipeline_run_id}}',
    ranked.source_file,
    ranked.source_row_number,
    ranked.source_record_id,
    'DUPLICATE_EVENT_DROPPED',
    'event_id duplikat; baris dengan ingested_at_utc paling baru dipertahankan',
    ranked.payload
FROM (
        SELECT source_file,
            source_row_number,
            source_record_id,
            payload,
            ROW_NUMBER() OVER (
                PARTITION BY source_file,
                payload->>'event_id'
                ORDER BY (payload->>'ingested_at_utc')::timestamptz DESC NULLS LAST,
                    source_row_number DESC
            ) AS rn
        FROM bronze.raw_records
        WHERE source_file IN (
                'events/return_events.json',
                'events/support_events.json',
                'events/payment_events.json',
                'events/refund_events.json',
                'events/web_events.json'
            )
            AND payload->>'event_id' IS NOT NULL
    ) ranked
WHERE ranked.rn > 1;