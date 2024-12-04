
DECLARE
  pos_run_dt DATE;
  psd_run_dt DATE;
  POS_CORP_NM CONSTANT VARCHAR := 'KELLANOVA';
  METRC_EVENTS CONSTANT VARCHAR := 'EVENTS';
  METRC_INDV_CATG CONSTANT VARCHAR := '(Individual)';
  METRC_ALL_CATG CONSTANT VARCHAR := '(All)';
  METRC_EVENT_CATG CONSTANT VARCHAR := '(Event Categories)';
  METRC_POS_PRFM CONSTANT VARCHAR := 'POS_PRFM';
  METRC_OOS CONSTANT VARCHAR := 'OOS';

BEGIN

SELECT NVL(run_date, MAX(fisc_wk_end_dt)) INTO pos_run_dt
FROM fin_acctg_ops.ref_fisc_cal_day
WHERE fisc_dt = CURRENT_DATE - INTERVAL '14 DAYS'
;

SELECT NVL(run_date, MAX(fisc_wk_end_dt)) INTO psd_run_dt
FROM fin_acctg_ops.ref_fisc_cal_day
WHERE fisc_dt = CURRENT_DATE - INTERVAL '14 DAYS'
;

-- To be used in KEY_BRAND, POS (timeframe + weekly), POS_PRFM, SHP
DROP TABLE IF EXISTS pos_pd_ref;
CREATE TEMP TABLE pos_pd_ref AS (
  SELECT
    MIN(fisc_wk_start_dt) AS pd_start_dt,
    MIN(fisc_yr_wk) AS pd_start_yr_wk,
    MAX(fisc_wk_end_dt) AS pd_end_dt,
    MAX(fisc_yr_wk) AS pd_end_yr_wk,
    'L4W' AS tmfrm_cd
  FROM fin_acctg_ops.ref_fisc_cal_day
  WHERE fisc_dt BETWEEN pos_run_dt - INTERVAL '3 WEEKS' AND pos_run_dt
  UNION ALL
  SELECT
    MIN(fisc_wk_start_dt) AS pd_start_dt,
    MIN(fisc_yr_wk) AS pd_start_yr_wk,
    MAX(fisc_wk_end_dt) AS pd_end_dt,
    MAX(fisc_yr_wk) AS pd_end_yr_wk,
    'L13W' AS tmfrm_cd
  FROM fin_acctg_ops.ref_fisc_cal_day
  WHERE fisc_dt BETWEEN pos_run_dt - INTERVAL '12 WEEKS' AND pos_run_dt
  UNION ALL
  SELECT
    MIN(fisc_wk_start_dt) AS pd_start_dt,
    MIN(fisc_yr_wk) AS pd_start_yr_wk,
    MAX(fisc_wk_end_dt) AS pd_end_dt,
    MAX(fisc_yr_wk) AS pd_end_yr_wk,
    'YTD' AS tmfrm_cd
  FROM fin_acctg_ops.ref_fisc_cal_day
  WHERE
    fisc_dt <= pos_run_dt
    AND fisc_yr = (SELECT DISTINCT fisc_yr FROM fin_acctg_ops.ref_fisc_cal_day WHERE fisc_dt = pos_run_dt)
);

DROP TABLE IF EXISTS pos_wkly_pd_ref;
CREATE TEMP TABLE pos_wkly_pd_ref AS (
  SELECT DISTINCT
    fisc_wk_start_dt as pd_start_dt,
    fisc_yr_wk AS pd_start_yr_wk,
    fisc_wk_end_dt AS pd_end_dt,
    fisc_yr_wk AS pd_end_yr_wk,
    'WEEKLY_L13W' AS tmfrm_cd
  FROM fin_acctg_ops.ref_fisc_cal_day
  WHERE fisc_dt BETWEEN pos_run_dt - INTERVAL '12 WEEKS' AND pos_run_dt
  UNION ALL
  SELECT DISTINCT
    fisc_wk_start_dt as pd_start_dt,
    fisc_yr_wk AS pd_start_yr_wk,
    fisc_wk_end_dt AS pd_end_dt,
    fisc_yr_wk AS pd_end_yr_wk,
    'WEEKLY_N13W' AS tmfrm_cd
  FROM fin_acctg_ops.ref_fisc_cal_day
  WHERE fisc_dt BETWEEN pos_run_dt - INTERVAL '51 WEEKS' AND pos_run_dt - INTERVAL '39 WEEKS'
);

-- To be used in EVENTS, OOS
DROP TABLE IF EXISTS psd_pd_ref;
CREATE TEMP TABLE psd_pd_ref AS (
  SELECT
    MIN(fisc_wk_start_dt) AS pd_start_dt,
    MIN(fisc_yr_wk) AS pd_start_yr_wk,
    MAX(fisc_wk_end_dt) AS pd_end_dt,
    MAX(fisc_yr_wk) AS pd_end_yr_wk,
    'L4W' AS tmfrm_cd
  FROM fin_acctg_ops.ref_fisc_cal_day
  WHERE fisc_dt BETWEEN psd_run_dt - INTERVAL '3 WEEKS' AND psd_run_dt
  UNION ALL
  SELECT
    MIN(fisc_wk_start_dt) AS pd_start_dt,
    MIN(fisc_yr_wk) AS pd_start_yr_wk,
    MAX(fisc_wk_end_dt) AS pd_end_dt,
    MAX(fisc_yr_wk) AS pd_end_yr_wk,
    'L13W' AS tmfrm_cd
  FROM fin_acctg_ops.ref_fisc_cal_day
  WHERE fisc_dt BETWEEN psd_run_dt - INTERVAL '12 WEEKS' AND psd_run_dt
  UNION ALL
  SELECT
    MIN(fisc_wk_start_dt) AS pd_start_dt,
    MIN(fisc_yr_wk) AS pd_start_yr_wk,
    MAX(fisc_wk_end_dt) AS pd_end_dt,
    MAX(fisc_yr_wk) AS pd_end_yr_wk,
    'YTD' AS tmfrm_cd
  FROM fin_acctg_ops.ref_fisc_cal_day
  WHERE
    fisc_dt <= psd_run_dt
    AND fisc_yr = (SELECT DISTINCT fisc_yr FROM fin_acctg_ops.ref_fisc_cal_day WHERE fisc_dt = psd_run_dt)
);

-- create a placeholder for all data
DROP TABLE IF EXISTS spr_tmfrm_sumry_final;
CREATE TEMP TABLE spr_tmfrm_sumry_final AS (
  SELECT * FROM sales_exec.dm_retl_exec_store_prfmnc_tmfrm_sumry WHERE 1=2
);

RAISE INFO 'Getting hierarchy and source preference';
CALL sales_strat_plan.sp_hier_post_spin(); -- getting the product hierarchy
CALL sales_strat_plan.sp_pos_sales_src_pref_temp(); -- getting the product hierarchy

RAISE INFO 'Creating store and product master lists';
-- Master list of stores
DROP TABLE IF EXISTS spr_store_mstr;
CREATE TEMP TABLE spr_store_mstr AS (
  WITH all_stores AS (
    -- create master list of stores
    SELECT DISTINCT
      cust.hier_b_rgn_nm AS rgn_nm,
      cust.hier_b_rgn_nbr AS rgn_nbr,
      cust.hier_b_area_nm AS zn_nm,
      cust.hier_b_area_nbr AS zn_nbr,
      cust.hier_b_terr_nm AS terr_nm,
      cust.hier_b_terr_nbr AS terr_nbr,
      cust.hier_d_sales_mgmt_a_nbr AS chnl_nbr,
      cust.hier_d_sales_mgmt_a_nm AS chnl_nm,
      cust.plan_to_nm,
      cust.plan_to_nbr,
      cust.chain_nm,
      cust.chain_nbr,
      cust.sales_org_cd,
      cust.sold_to_nm,
      cust.sold_to_nbr,
      (cust.sold_to_nm || '-' || cust.sold_to_nbr) AS sold_to_desc,
      cust.tdlinx_nbr,
      cust.sold_to_nm AS store_nm,
      cust.street_nm,
      cust.city_nm,
      cust.rgn_cd,
      cust.pstl_cd,
      cust.tdlinx_store_nbr AS store_nbr,
      cust.tdlinx_nbr AS prev_acct_nbr,
      '' AS vndr_nm,
      CASE WHEN NVL(cust.hier_b_level4_misc_nbr, '') != '' THEN 1 ELSE 0 END AS retl_ind,
      CASE WHEN NVL(cust.hier_b_level4_misc_nbr, '') = '' AND sales_org_order_blok_cd = '01' AND order_blok_cd = '04' THEN 1 ELSE 0 END AS non_retl_ind,
      cust.src_nm
    FROM cust_mstr.cust_sold_to_pivot cust
    INNER JOIN pos_sales_src_pref_temp pref USING (plan_to_nbr)
    WHERE
      -- NVL(tdlinx_store_nbr, '') != ''
      sales_org_cd = '1001'
      -- Filtering the channel name
      AND UPPER(hier_d_sales_mgmt_a_nm) NOT IN ('CANADA L3', 'REMARKETING', 'PUREPLAY E-COMMERCE', 'SPECIALTY')
      -- Filtering the inactive plan to and stores
      AND UPPER(cust.plan_to_nm) NOT LIKE '%INACT%' AND UPPER(sold_to_nm) NOT LIKE '%INACT%'
    -- AND sold_to_nbr = '1000001054' -- meijer stores
  ),
  retail_covered_stores AS (
    SELECT
      'REX_COVERED' AS hier_type_cd,
      *
    FROM all_stores
    WHERE retl_ind = 1
  ),
  retail_non_covered_stores AS (
    SELECT
      'REX_NON_COVERED' AS hier_type_cd,
      *
    FROM all_stores
    WHERE retl_ind = 1
    UNION ALL
    SELECT
      'REX_NON_COVERED' AS hier_type_cd,
      *
    FROM all_stores
    WHERE retl_ind = 0 AND non_retl_ind = 1
  )
  SELECT * FROM retail_covered_stores
  UNION ALL
  SELECT * FROM retail_non_covered_stores
);

DROP TABLE IF EXISTS store_prod_mstr;
CREATE TEMP TABLE store_prod_mstr AS (
  WITH prod_mstr AS (
    SELECT DISTINCT
      -- gtin,
      NVL(catg_nm, '') AS catg_nm,
      CASE WHEN NVL(immed_cnsmptn_prod_type_cd, '') != '' THEN 'Immediate Consumption' ELSE NVL(catg_nm, '') END AS ic_catg_nm,
      NVL(brand_nm, '') AS brand_nm
    FROM prod_hier_post_spin_gtin
    WHERE
      gtin IS NOT NULL AND TRIM(gtin) != ''
      AND UPPER(comrcl_catg_cd) != 'WIP' AND catg_cd NOT IN ('2040') -- Fruit Snacks
      AND (spin_ind = 1 OR klnva_retn_brand_ind = 1)    -- filter out WKKC or outdated data
  )
  SELECT DISTINCT
    sold_to_nbr,
    sold_to_nm,
    tdlinx_nbr,
    plan_to_nbr,
    plan_to_nm,
    chain_nbr,
    hier_type_cd,
    catg_nm,
    ic_catg_nm,
    brand_nm,
    src_nm
  FROM spr_store_mstr
  CROSS JOIN prod_mstr
);

/***************************************************
*************  SHIPMENT SUMMARY START  *************
****************************************************/

RAISE INFO 'Creating Shipment summary';
-- Creating shipment timeframe summary
DROP TABLE IF EXISTS gsv_tmfrm_sumry;
CREATE TEMP TABLE gsv_tmfrm_sumry AS (
  WITH matrl_hier AS (
    SELECT DISTINCT
      matrl_nbr,
      catg_nm,
      brand_nm
    FROM prod_hier_post_spin_matrl
    WHERE (spin_ind = 1 OR klnva_retn_brand_ind = 1) AND comrcl_catg_cd != 'WIP' AND catg_cd NOT IN ('2040') -- Fruit Snacks
      AND brand_nm IS NOT NULL
  ),
  cust_hier AS (
    SELECT DISTINCT 
      ISNULL(sold_to_nbr, '') AS sold_to_nbr,
      ISNULL(plan_to_nbr, '') AS plan_to_nbr,
      ISNULL(chain_nbr, '') AS chain_nbr,
      chain_nm
    FROM
      cust_mstr.cust_sold_to_pivot
    WHERE 
      sales_org_cd = '1001' -- Kellanova org code
      -- Filtering the channel name
      AND UPPER(hier_d_sales_mgmt_a_nm) NOT IN (
        'CANADA L3',
        'REMARKETING',
        'PUREPLAY E-COMMERCE',
        'SPECIALTY'
      ) 
       -- Filtering the inactive plans
      AND UPPER(plan_to_nm) NOT LIKE '%INACTIVE%'
      AND UPPER(sold_to_nm) NOT LIKE '%INACTIVE%'
  ),
  gsv_details AS (
    SELECT
      cust.plan_to_nbr,
      phr.catg_nm,
      phr.brand_nm,
      fc.fisc_yr_wk,
      fc.fisc_wk_end_dt,
      dir_indir_gsv,
      copa.src_nm,
      copa.kortex_upld_ts,
      copa.kortex_dprct_ts
    FROM fin_plan_analys.fin_prfmnc_mgmt_copa_cmpnt_detl copa
    INNER JOIN fin_acctg_ops.ref_fisc_cal_wk fc ON copa.fisc_yr::INT = fc.fisc_yr AND copa.fisc_wk::INT = fc.fisc_wk
    INNER JOIN cust_hier cust ON (copa.sold_to_nbr = cust.sold_to_nbr)
    INNER JOIN matrl_hier phr USING (matrl_nbr)
    WHERE
      fisc_wk_end_dt BETWEEN (SELECT MIN(pd_start_dt) - INTERVAL '52 WEEKS' FROM pos_pd_ref) AND (SELECT MAX(pd_end_dt) FROM pos_pd_ref)
      -- AND retl_ind = 1
      AND copa.sales_org_cd = '1001'
  ),
  cur_yr_gsv AS (
    SELECT
      tmfrm_cd,
      plan_to_nbr,
      catg_nm,
      brand_nm,
      SUM(dir_indir_gsv) AS gsv,
      src_nm,
      MAX(kortex_upld_ts) AS kortex_upld_ts,
      MAX(kortex_dprct_ts) AS kortex_dprct_ts
    FROM gsv_details copa
    INNER JOIN pos_pd_ref ON fisc_yr_wk BETWEEN pd_start_yr_wk AND pd_end_yr_wk
    GROUP BY
      tmfrm_cd,
      plan_to_nbr,
      catg_nm,
      brand_nm,
      src_nm
  ),
  prev_yr_gsv AS (
    SELECT
      tmfrm_cd,
      plan_to_nbr,
      catg_nm,
      brand_nm,
      SUM(dir_indir_gsv) AS gsv_yr_ago
    FROM gsv_details copa
    INNER JOIN pos_pd_ref ON fisc_wk_end_dt BETWEEN pd_start_dt - INTERVAL '52 WEEKS' AND pd_end_dt - INTERVAL '52 WEEKS'
    GROUP BY
      tmfrm_cd,
      plan_to_nbr,
      catg_nm,
      brand_nm,
      src_nm
  )
  SELECT
    mstr.sold_to_nbr,
    mstr.plan_to_nbr,
    mstr.chain_nbr,
    pd.tmfrm_cd,
    phr.catg_nm,
    phr.brand_nm,
    NVL(cur.gsv, 0) AS gsv,
    NVL(ya.gsv_yr_ago, 0) AS gsv_yr_ago,
    NVL(cur.src_nm, mstr.src_nm) AS src_nm,
    cur.kortex_upld_ts,
    cur.kortex_dprct_ts
  FROM spr_store_mstr mstr
  CROSS JOIN (SELECT DISTINCT catg_nm, brand_nm FROM matrl_hier) phr
  CROSS JOIN (SELECT DISTINCT tmfrm_cd FROM cur_yr_gsv) pd
  LEFT JOIN cur_yr_gsv cur USING (plan_to_nbr, catg_nm, brand_nm, tmfrm_cd)
  LEFT JOIN prev_yr_gsv ya USING (plan_to_nbr, catg_nm, brand_nm, tmfrm_cd)
  WHERE hier_type_cd = 'REX_COVERED'
);

-- SHP
DELETE FROM spr_tmfrm_sumry_final WHERE metrc_nm = 'SHP';
INSERT INTO spr_tmfrm_sumry_final (
  metrc_nm,
  tmfrm_cd,
  sold_to_nbr,
  plan_to_nbr,
  chain_nbr,
  catg_nm,
  brand_nm,
  sale_val,
  sale_yr_ago_val,
  src_nm,
  kortex_upld_ts,
  kortex_dprct_ts
  )
  SELECT
    'SHP' AS metrc_nm,
    tmfrm_cd,
    sold_to_nbr,
    plan_to_nbr,
    chain_nbr,
    catg_nm,
    brand_nm,
    gsv AS sale_val,
    gsv_yr_ago AS sale_yr_ago_val,
    src_nm,
    kortex_upld_ts,
    kortex_dprct_ts
  FROM gsv_tmfrm_sumry
;
DROP TABLE IF EXISTS gsv_tmfrm_sumry;

------------------- SHIPMENT END -------------------

/***************************************************
*****************  FETCH POS DATA  *****************
****************************************************/


RAISE INFO 'Fetching POS timeframe summary';
-- create pos timeframe summary for L4W, L13W, YTD, WEEKLY_L13W, WEEKLY_N13W
DROP TABLE IF EXISTS pos_tmfrm_sumry_ic;
CREATE TEMP TABLE pos_tmfrm_sumry_ic AS (
  SELECT
    tmfrm_cd,
    pd_end_yr_wk,
    pd_end_dt,
    mstr.sold_to_nbr,
    mstr.chain_nbr,
    CASE WHEN NVL(immed_cnsmptn_prod_type_cd, '') != '' THEN 'Immediate Consumption' ELSE catg_nm END AS ic_catg_nm,
    brand_nm,
    corp_nm,
    -- MAX(wk_nbr) AS tmfrm_wk_nbr,
    SUM(sale_val) AS sale_val,
    SUM(sale_yr_ago_val) AS sale_yr_ago_val,
    SUM(sale_qty) AS sale_qty,
    SUM(sale_yr_ago_qty) AS sale_yr_ago_qty,
    SUM(sale_vol_lb_val) AS sale_vol_lb_val,
    SUM(sale_yr_ago_vol_lb_val) AS sale_yr_ago_vol_lb_val,
    MAX(pos.src_nm) AS src_nm,
    MAX(kortex_upld_ts) AS kortex_upld_ts,
    MAX(kortex_dprct_ts) AS kortex_dprct_ts
  FROM sales_prfmnc_eval.dm_mkt_prfmnc_pos_store_catg_wkly_sumry pos
  INNER JOIN (SELECT * FROM pos_pd_ref UNION ALL SELECT * FROM pos_wkly_pd_ref) ON fisc_yr_wk BETWEEN pd_start_yr_wk AND pd_end_yr_wk
  INNER JOIN spr_store_mstr mstr USING (sold_to_nbr)
  WHERE
    (corp_nm = POS_CORP_NM OR tmfrm_cd NOT LIKE 'WEEKLY%')
    AND hier_type_cd = 'REX_NON_COVERED'
    AND pos.plan_to_nbr NOT IN ( '0000300850') -- Publix because data is interpolated
    AND (
      (corp_nm = POS_CORP_NM AND catg_nm IN (SELECT DISTINCT catg_nm FROM store_prod_mstr))
      OR (corp_nm != POS_CORP_NM AND catg_nm != 'Others')
    )
  GROUP BY
    tmfrm_cd,
    pd_end_yr_wk,
    pd_end_dt,
    mstr.sold_to_nbr,
    mstr.chain_nbr,
    CASE WHEN NVL(immed_cnsmptn_prod_type_cd, '') != '' THEN 'Immediate Consumption' ELSE catg_nm END,
    brand_nm,
    corp_nm
);

DROP TABLE IF EXISTS pos_tmfrm_sumry_without_ic;
CREATE TEMP TABLE pos_tmfrm_sumry_without_ic AS (
  SELECT
    tmfrm_cd,
    pd_end_yr_wk,
    pd_end_dt,
    mstr.sold_to_nbr,
    mstr.chain_nbr,
    catg_nm,
    brand_nm,
    corp_nm,
    -- MAX(wk_nbr) AS tmfrm_wk_nbr,
    SUM(sale_val) AS sale_val,
    SUM(sale_yr_ago_val) AS sale_yr_ago_val,
    SUM(sale_qty) AS sale_qty,
    SUM(sale_yr_ago_qty) AS sale_yr_ago_qty,
    SUM(sale_vol_lb_val) AS sale_vol_lb_val,
    SUM(sale_yr_ago_vol_lb_val) AS sale_yr_ago_vol_lb_val,
    MAX(pos.src_nm) AS src_nm,
    MAX(kortex_upld_ts) AS kortex_upld_ts,
    MAX(kortex_dprct_ts) AS kortex_dprct_ts
  FROM sales_prfmnc_eval.dm_mkt_prfmnc_pos_store_catg_wkly_sumry pos
  INNER JOIN (SELECT * FROM pos_pd_ref) ON fisc_yr_wk BETWEEN pd_start_yr_wk AND pd_end_yr_wk
  INNER JOIN spr_store_mstr mstr USING (sold_to_nbr)
  WHERE
    -- corp_nm = POS_CORP_NM
    hier_type_cd = 'REX_NON_COVERED'
    AND pos.plan_to_nbr NOT IN ('0000300850') -- Publix because data is interpolated
    AND (catg_nm IN (SELECT DISTINCT catg_nm FROM store_prod_mstr) OR catg_nm = 'Frozen Meals')
  GROUP BY
    tmfrm_cd,
    pd_end_yr_wk,
    pd_end_dt,
    mstr.sold_to_nbr,
    mstr.hier_type_cd,
    mstr.chain_nbr,
    catg_nm,
    brand_nm,
    corp_nm
);


/***************************************************
****************  KEY BRAND START  *****************
****************************************************/

RAISE INFO 'Creating Key Brand summary';
-- Creating KEY_BRAND summary
DROP TABLE IF EXISTS key_brand_tmfrm_sumry;
CREATE TEMP TABLE key_brand_tmfrm_sumry AS (
  WITH key_brand_sale AS (
    SELECT
      sm.type_cd,
      sm.sold_to_nbr,
      sm.chain_nbr,
      sm.ic_catg_nm AS catg_nm,
      sm.brand_nm,
      POS_CORP_NM AS corp_nm,
      sm.tmfrm_cd,
      NVL(SUM(sale_val), 0) AS sale_val,
      NVL(SUM(sale_yr_ago_val), 0) AS sale_yr_ago_val,
      NVL(SUM(sale_qty), 0) AS sale_qty,
      NVL(SUM(sale_yr_ago_qty), 0) AS sale_yr_ago_qty,
      sm.src_nm,
      MAX(pos.kortex_upld_ts) AS kortex_upld_ts,
      MAX(pos.kortex_dprct_ts) AS kortex_dprct_ts
    FROM (
      SELECT * FROM (
      SELECT DISTINCT sold_to_nbr, chain_nbr, ic_catg_nm, brand_nm, 'Brands' AS type_cd, src_nm FROM store_prod_mstr WHERE hier_type_cd = 'REX_NON_COVERED'
      UNION ALL
      SELECT DISTINCT sold_to_nbr, chain_nbr, ic_catg_nm, 'KEY' AS brand_nm, 'Categories' AS type_cd, src_nm FROM store_prod_mstr WHERE hier_type_cd = 'REX_NON_COVERED'
      ) CROSS JOIN (SELECT DISTINCT tmfrm_cd FROM pos_pd_ref)
    ) sm
    LEFT JOIN pos_tmfrm_sumry_ic pos USING (sold_to_nbr, ic_catg_nm, tmfrm_cd)
    WHERE (corp_nm = POS_CORP_NM OR corp_nm IS NULL)
      AND (sm.brand_nm = pos.brand_nm OR sm.brand_nm = 'KEY')
    GROUP BY
      sm.type_cd,
      sm.sold_to_nbr,
      sm.chain_nbr,
      sm.ic_catg_nm,
      sm.brand_nm,
      sm.tmfrm_cd,
      sm.src_nm
  ),
  store_key_brands AS (
    SELECT
      bd.*,
      ROW_NUMBER() OVER (PARTITION BY type_cd, sold_to_nbr, CASE WHEN type_cd = 'Brands' THEN catg_nm END, tmfrm_cd ORDER BY sale_val DESC, sale_yr_ago_val DESC) AS brand_sale_rank,
      -- brand_limit AS allowed_brand_rank,
      SUM(sale_val) OVER(PARTITION BY type_cd, sold_to_nbr, CASE WHEN type_cd = 'Brands' THEN catg_nm END, tmfrm_cd) AS store_sale_val,
      SUM(sale_qty) OVER(PARTITION BY type_cd, sold_to_nbr, CASE WHEN type_cd = 'Brands' THEN catg_nm END, tmfrm_cd) AS store_sale_qty,
      SUM(sale_val) OVER(PARTITION BY type_cd, chain_nbr, brand_nm, tmfrm_cd) AS chain_brand_sale_val,
      SUM(sale_yr_ago_val) OVER(PARTITION BY type_cd, chain_nbr, brand_nm, tmfrm_cd) AS chain_brand_sale_yr_ago_val,
      CASE WHEN store_sale_val = 0 THEN 0 ELSE 100.0 * sale_val/store_sale_val END AS sale_val_share_pct,
      (sale_val - sale_yr_ago_val) AS sale_vs_yr_ago_val,
      CASE WHEN sale_yr_ago_val = 0 THEN 0 ELSE 100.0 * sale_vs_yr_ago_val/sale_yr_ago_val END AS sale_vs_yr_ago_val_pct,
      CASE WHEN store_sale_qty = 0 THEN 0 ELSE 100.0 * sale_qty/store_sale_qty END AS sale_qty_share_pct,
      (sale_qty - sale_yr_ago_qty) AS sale_vs_yr_ago_qty,
      CASE WHEN sale_yr_ago_qty = 0 THEN 0 ELSE 100.0 * sale_vs_yr_ago_qty/sale_yr_ago_qty END AS sale_vs_yr_ago_qty_pct,
      actl_store_sale_val,
      actl_store_sale_yr_ago_val,
      actl_store_sale_qty,
      actl_store_sale_yr_ago_qty
    FROM key_brand_sale bd
    LEFT JOIN (
      SELECT
        sold_to_nbr,
        tmfrm_cd,
        SUM(sale_val) AS actl_store_sale_val,
        SUM(sale_yr_ago_val) AS actl_store_sale_yr_ago_val,
        SUM(sale_qty) AS actl_store_sale_qty,
        SUM(sale_yr_ago_qty) AS actl_store_sale_yr_ago_qty
      FROM key_brand_sale
      WHERE type_cd = 'Categories'
      GROUP BY
        sold_to_nbr,
        tmfrm_cd
    ) USING (sold_to_nbr, tmfrm_cd)
  )
  SELECT
    *,
    DENSE_RANK() OVER (
      PARTITION BY type_cd, chain_nbr, CASE WHEN type_cd = 'Brands' THEN catg_nm END, tmfrm_cd
      ORDER BY chain_brand_sale_val DESC, chain_brand_sale_yr_ago_val DESC
    ) AS chain_brand_sale_rank
  FROM store_key_brands
);

DELETE FROM spr_tmfrm_sumry_final WHERE metrc_nm = 'KEY_BRAND';
INSERT INTO spr_tmfrm_sumry_final (
  hier_type_cd,
  metrc_nm,
  tmfrm_cd,
  sold_to_nbr,
  chain_nbr,
  catg_nm,
  mult_catg_nm,
  brand_nm,
  corp_nm,
  sale_val,
  sale_yr_ago_val,
  sale_qty,
  sale_yr_ago_qty,
  brand_sale_rank,
  -- allowed_brand_rank,
  sale_val_share_pct,
  sale_vs_yr_ago_val,
  sale_vs_yr_ago_val_pct,
  store_sale_val,
  store_sale_yr_ago_val,
  store_sale_qty,
  store_sale_yr_ago_qty,
  src_nm,
  kortex_upld_ts,
  kortex_dprct_ts
  )
  SELECT
    hier_type_cd,
    'KEY_BRAND' AS metrc_nm,
    tmfrm_cd,
    sold_to_nbr,
    chain_nbr,
    catg_nm,
    NULL AS mult_catg_nm,
    CASE
      WHEN hier_type_cd = 'REX_NON_COVERED' AND brand_nm = 'KEY' THEN 'TOTAL'
      ELSE brand_nm
    END AS brand_nm,
    corp_nm,
    sale_val,
    sale_yr_ago_val,
    sale_qty,
    sale_yr_ago_qty,
    CASE
      WHEN hier_type_cd = 'REX_NON_COVERED' THEN chain_brand_sale_rank
      ELSE brand_sale_rank
    END AS brand_sale_rank,
    -- allowed_brand_rank,
    sale_val_share_pct,
    sale_vs_yr_ago_val,
    sale_vs_yr_ago_val_pct,
    -- sale_qty_share_pct,
    -- sale_vs_yr_ago_qty,
    -- sale_vs_yr_ago_qty_pct,
    actl_store_sale_val,
    actl_store_sale_yr_ago_val,
    actl_store_sale_qty,
    actl_store_sale_yr_ago_qty,
    src_nm,
    kortex_upld_ts,
    kortex_dprct_ts
  FROM key_brand_tmfrm_sumry
  CROSS JOIN (SELECT DISTINCT hier_type_cd FROM spr_store_mstr)
;
-- DROP TABLE IF EXISTS key_brand_tmfrm_sumry;

------------------ KEY BRAND END ------------------

/***************************************************
***************  POS SUMMARY START  ****************
****************************************************/

RAISE INFO 'Creating POS timeframe final summary';
-- creating pos timeframe summary
DROP TABLE IF EXISTS pos_tmfrm_sumry_final;
CREATE TEMP TABLE pos_tmfrm_sumry_final AS (
  WITH master_list AS (
    SELECT DISTINCT hier_type_cd, sold_to_nbr, chain_nbr, ic_catg_nm, brand_nm, tmfrm_cd, pd_end_yr_wk, src_nm
    FROM store_prod_mstr
    CROSS JOIN pos_pd_ref
  ),
  store_catg_facts AS (
    SELECT
      mstr.hier_type_cd,
      NVL(pos.corp_nm, POS_CORP_NM) AS corp_nm,
      mstr.sold_to_nbr,
      mstr.chain_nbr,
      mstr.ic_catg_nm,
      mstr.brand_nm,
      mstr.tmfrm_cd,
      mstr.pd_end_yr_wk,
      NVL(sale_val, 0) AS sale_val,
      NVL(sale_yr_ago_val, 0) AS sale_yr_ago_val,
      NVL(sale_qty, 0) AS sale_qty,
      NVL(sale_yr_ago_qty, 0) AS sale_yr_ago_qty,
      mstr.src_nm
    FROM master_list mstr
    LEFT JOIN pos_tmfrm_sumry_ic pos USING (sold_to_nbr, ic_catg_nm, brand_nm, tmfrm_cd, pd_end_yr_wk)
    WHERE pos.corp_nm = POS_CORP_NM OR pos.corp_nm IS NULL
    UNION ALL
    SELECT
      mstr.hier_type_cd,
      pos.corp_nm,
      pos.sold_to_nbr,
      pos.chain_nbr,
      pos.ic_catg_nm,
      pos.brand_nm,
      pos.tmfrm_cd,
      pos.pd_end_yr_wk,
      pos.sale_val,
      pos.sale_yr_ago_val,
      pos.sale_qty,
      pos.sale_yr_ago_qty,
      pos.src_nm
    FROM pos_tmfrm_sumry_ic pos
    INNER JOIN spr_store_mstr mstr USING (sold_to_nbr)
    WHERE pos.corp_nm != POS_CORP_NM
  ),
  pos_calc_metrics AS (
    SELECT
      'facts' AS agg_type,
      hier_type_cd, -- retail_covered/retail_non_covered
      corp_nm,
      sold_to_nbr,
      chain_nbr,
      ic_catg_nm,
      brand_nm,
      tmfrm_cd,
      pd_end_yr_wk,
      sale_val,
      sale_yr_ago_val,
      sale_qty,
      sale_yr_ago_qty,
      SUM(sale_val) OVER(PARTITION BY hier_type_cd, sold_to_nbr, ic_catg_nm, tmfrm_cd, pd_end_yr_wk) AS total_catg_sale_val,
      SUM(sale_yr_ago_val) OVER(PARTITION BY hier_type_cd, sold_to_nbr, ic_catg_nm, tmfrm_cd, pd_end_yr_wk) AS total_catg_sale_yr_ago_val,
      CASE WHEN cmp.sold_to_nbr IS NULL OR total_catg_sale_val = 0 THEN 0 ELSE 100.0*(sale_val / total_catg_sale_val) END AS sale_val_share_pct,
      CASE WHEN cmp.sold_to_nbr IS NULL OR total_catg_sale_yr_ago_val = 0 THEN 0 ELSE 100.0*(sale_yr_ago_val / total_catg_sale_yr_ago_val) END AS sale_yr_ago_val_share_pct,
      SUM(sale_val) OVER(PARTITION BY hier_type_cd, sold_to_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk) AS store_sale_val,
      SUM(sale_yr_ago_val) OVER(PARTITION BY hier_type_cd, sold_to_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk) AS store_sale_yr_ago_val,
      SUM(sale_qty) OVER(PARTITION BY hier_type_cd, sold_to_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk) AS store_sale_qty,
      SUM(sale_yr_ago_qty) OVER(PARTITION BY hier_type_cd, sold_to_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk) AS store_sale_yr_ago_qty,
      CASE WHEN store_sale_yr_ago_val = 0 THEN 0 ELSE 100.0 * (store_sale_val - store_sale_yr_ago_val) / store_sale_yr_ago_val END AS store_sale_vs_yr_ago_val_pct,
      CASE WHEN store_sale_yr_ago_qty = 0 THEN 0 ELSE 100.0 * (store_sale_qty - store_sale_yr_ago_qty) / store_sale_yr_ago_qty END AS store_sale_vs_yr_ago_qty_pct,
      SUM(sale_val) OVER(PARTITION BY hier_type_cd, chain_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk) AS chain_sale_val,
      SUM(sale_yr_ago_val) OVER(PARTITION BY hier_type_cd, chain_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk) AS chain_sale_yr_ago_val,
      SUM(sale_qty) OVER(PARTITION BY hier_type_cd, chain_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk) AS chain_sale_qty,
      SUM(sale_yr_ago_qty) OVER(PARTITION BY hier_type_cd, chain_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk) AS chain_sale_yr_ago_qty,
      CASE WHEN chain_sale_yr_ago_val = 0 THEN 0 ELSE 100.0 * (chain_sale_val - chain_sale_yr_ago_val) / chain_sale_yr_ago_val END AS chain_sale_vs_yr_ago_val_pct,
      CASE WHEN chain_sale_yr_ago_qty = 0 THEN 0 ELSE 100.0 * (chain_sale_qty - chain_sale_yr_ago_qty) / chain_sale_yr_ago_qty END AS chain_sale_vs_yr_ago_qty_pct,
      src_nm
    FROM store_catg_facts
    LEFT JOIN (SELECT DISTINCT sold_to_nbr FROM store_catg_facts WHERE corp_nm != POS_CORP_NM) cmp USING (sold_to_nbr)
    UNION ALL
    SELECT
      'mstr' AS agg_type,
      hier_type_cd, -- retail_covered/retail_non_covered
      POS_CORP_NM AS corp_nm,
      sold_to_nbr,
      chain_nbr,
      ic_catg_nm,
      NULL AS brand_nm,
      tmfrm_cd,
      pd_end_yr_wk,
      NULL::FLOAT8 AS sale_val,
      NULL::FLOAT8 AS sale_yr_ago_val,
      NULL::FLOAT8 AS sale_qty,
      NULL::FLOAT8 AS sale_yr_ago_qty,
      NULL::FLOAT8 AS total_catg_sale_val,
      NULL::FLOAT8 AS total_catg_sale_yr_ago_val,
      NULL::FLOAT8 AS sale_val_share_pct,
      NULL::FLOAT8 AS sale_yr_ago_val_share_pct,
      NULL::FLOAT8 AS store_sale_val,
      NULL::FLOAT8 AS store_sale_yr_ago_val,
      NULL::FLOAT8 AS store_sale_qty,
      NULL::FLOAT8 AS store_sale_yr_ago_qty,
      NULL::FLOAT8 AS store_sale_vs_yr_ago_val_pct,
      NULL::FLOAT8 AS store_sale_vs_yr_ago_qty_pct,
      NULL::FLOAT8 AS chain_sale_val,
      NULL::FLOAT8 AS chain_sale_yr_ago_val,
      NULL::FLOAT8 AS chain_sale_qty,
      NULL::FLOAT8 AS chain_sale_yr_ago_qty,
      NULL::FLOAT8 AS chain_sale_vs_yr_ago_val_pct,
      NULL::FLOAT8 AS chain_sale_vs_yr_ago_qty_pct,
      src_nm
    FROM spr_store_mstr
    CROSS JOIN pos_pd_ref
    CROSS JOIN (SELECT METRC_ALL_CATG AS ic_catg_nm UNION SELECT METRC_EVENT_CATG)
  )
  SELECT
    calc.*,
    rk.chain_store_sale_val_rank,
    rk.chain_store_sale_qty_rank,
    sc.chain_store_cnt
  FROM pos_calc_metrics calc
  LEFT JOIN (
    SELECT
      *,
      RANK() OVER (PARTITION BY hier_type_cd, chain_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk ORDER BY store_sale_vs_yr_ago_val_pct DESC) AS chain_store_sale_val_rank,
      RANK() OVER (PARTITION BY hier_type_cd, chain_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk ORDER BY store_sale_vs_yr_ago_qty_pct DESC) AS chain_store_sale_qty_rank
    FROM (
      SELECT DISTINCT
        hier_type_cd, sold_to_nbr, chain_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk, store_sale_vs_yr_ago_val_pct, store_sale_vs_yr_ago_qty_pct
      FROM pos_calc_metrics WHERE agg_type = 'facts'
    )
  ) rk USING (hier_type_cd, sold_to_nbr, chain_nbr, corp_nm, tmfrm_cd, pd_end_yr_wk)
  LEFT JOIN (
    SELECT hier_type_cd, chain_nbr, COUNT(DISTINCT sold_to_nbr) AS chain_store_cnt FROM spr_store_mstr GROUP BY hier_type_cd, chain_nbr
  ) sc USING (hier_type_cd, chain_nbr)
);

-- Insert timeframe POS summary
DELETE FROM spr_tmfrm_sumry_final WHERE metrc_nm = 'POS' AND sub_metrc_nm IS NULL AND sold_to_nbr != '';
INSERT INTO spr_tmfrm_sumry_final (
    hier_type_cd,
    metrc_nm,
    sub_metrc_nm,
    tmfrm_cd,
    tmfrm_desc,
    corp_nm,
    sold_to_nbr,
    chain_nbr,
    catg_nm,
    brand_nm,
    sale_val,
    sale_yr_ago_val,
    sale_qty,
    sale_yr_ago_qty,
    sale_val_share_pct,
    sale_yr_ago_val_share_pct,
    store_sale_val,
    store_sale_yr_ago_val,
    store_sale_qty,
    store_sale_yr_ago_qty,
    store_sale_vs_yr_ago_val_pct,
    store_sale_vs_yr_ago_qty_pct,
    chain_sale_val,
    chain_sale_yr_ago_val,
    chain_sale_qty,
    chain_sale_yr_ago_qty,
    chain_sale_vs_yr_ago_val_pct,
    chain_sale_vs_yr_ago_qty_pct,
    chain_store_sale_val_rank,
    chain_store_sale_qty_rank,
    chain_store_cnt,
    src_nm
  )
  SELECT
    hier_type_cd,
    'POS' AS metrc_nm, -- typ = 'POS'
    NULL AS sub_metrc_nm,
    tmfrm_cd,
    CASE tmfrm_cd
      WHEN 'L4W' THEN 'Last 4 Weeks'
      WHEN 'L13W' THEN 'Last 13 Weeks'
      WHEN 'YTD' THEN 'Year to Date'
    END AS tmfrm_desc,
    corp_nm,
    sold_to_nbr,
    chain_nbr,
    ic_catg_nm AS catg_nm,
    brand_nm,
    sale_val,
    sale_yr_ago_val,
    sale_qty,
    sale_yr_ago_qty,
    sale_val_share_pct,
    sale_yr_ago_val_share_pct,
    store_sale_val,
    store_sale_yr_ago_val,
    store_sale_qty,
    store_sale_yr_ago_qty,
    store_sale_vs_yr_ago_val_pct,
    store_sale_vs_yr_ago_qty_pct,
    chain_sale_val,
    chain_sale_yr_ago_val,
    chain_sale_qty,
    chain_sale_yr_ago_qty,
    chain_sale_vs_yr_ago_val_pct,
    chain_sale_vs_yr_ago_qty_pct,
    chain_store_sale_val_rank,
    chain_store_sale_qty_rank,
    chain_store_cnt,
    src_nm
  FROM pos_tmfrm_sumry_final
;

-- Insert Weekly POS summary
DELETE FROM spr_tmfrm_sumry_final WHERE metrc_nm = 'POS' AND sub_metrc_nm IS NOT NULL AND sold_to_nbr != '';
INSERT INTO spr_tmfrm_sumry_final (
    hier_type_cd,
    metrc_nm,
    sub_metrc_nm,
    fisc_yr,
    tmfrm_cd,
    tmfrm_desc,
    corp_nm,
    sold_to_nbr,
    chain_nbr,
    catg_nm,
    brand_nm,
    sale_val,
    sale_yr_ago_val,
    sale_qty,
    sale_yr_ago_qty,
    src_nm
  )
  SELECT
    mstr.hier_type_cd,
    'POS' AS metrc_nm, -- typ = 'POS'
    CASE mstr.tmfrm_cd
      WHEN 'WEEKLY_N13W' THEN 'Next 13 Weeks'
      WHEN 'WEEKLY_L13W' THEN 'Last 13 Weeks'
    END AS sub_metrc_nm, -- typ_graph = Next 13 Weeks, Last 13 Weeks
    LEFT(mstr.pd_end_yr_wk, 4)::INT AS fisc_yr, -- fisc_yr = 2023
    (RIGHT(mstr.pd_end_yr_wk, 2)::INT)::TEXT AS tmfrm_cd, -- fisc_wk = 1 to 52
    TO_CHAR(mstr.pd_start_dt, 'Mon DD, YYYY') AS tmfrm_desc, -- fisc_wk_strt = 'Jan 01, 2023'
    corp_nm,
    mstr.sold_to_nbr,
    mstr.chain_nbr,
    mstr.ic_catg_nm AS catg_nm,
    mstr.brand_nm,
    sale_val,
    sale_yr_ago_val,
    sale_qty,
    sale_yr_ago_qty,
    mstr.src_nm
  FROM (
    SELECT * FROM store_prod_mstr CROSS JOIN pos_wkly_pd_ref
  ) mstr
  LEFT JOIN pos_tmfrm_sumry_ic USING (sold_to_nbr, ic_catg_nm, brand_nm, tmfrm_cd, pd_end_yr_wk)
  WHERE corp_nm = POS_CORP_NM
;

-- DROP TABLE IF EXISTS pos_tmfrm_sumry_final;


----------------- POS SUMMARY END -----------------



/***************************************************
***********  POS Category SUMMARY START  ***********
****************************************************/

RAISE INFO 'Creating POS Category timeframe summary for AMPS';
-- creating pos timeframe summary
DROP TABLE IF EXISTS pos_catg_tmfrm_sumry;
CREATE TEMP TABLE pos_catg_tmfrm_sumry AS (
  WITH tmfrm_catg_sale AS (
    SELECT
      tmfrm_cd,
      pos.sold_to_nbr,
      catg_nm,
      corp_nm,
      SUM(sale_val) AS sale_val,
      SUM(sale_yr_ago_val) AS sale_yr_ago_val,
      MAX(pos.src_nm) AS src_nm
    FROM pos_tmfrm_sumry_without_ic pos
    INNER JOIN spr_store_mstr using (sold_to_nbr)
    WHERE hier_type_cd = 'REX_COVERED' AND tmfrm_cd IN ('L4W', 'L13W', 'YTD')
      AND sold_to_nbr IN (SELECT DISTINCT sold_to_nbr FROM pos_tmfrm_sumry_without_ic WHERE corp_nm != POS_CORP_NM)
      AND catg_nm IN (SELECT DISTINCT catg_nm FROM pos_tmfrm_sumry_without_ic WHERE corp_nm != POS_CORP_NM)
    GROUP BY
      tmfrm_cd,
      pos.sold_to_nbr,
      catg_nm,
      corp_nm
  )
  SELECT
    sale.*,
    SUM(sale_val) OVER (PARTITION BY sold_to_nbr, catg_nm, tmfrm_cd) AS total_catg_sale_val,
    SUM(sale_yr_ago_val) OVER (PARTITION BY sold_to_nbr, catg_nm, tmfrm_cd) AS total_catg_sale_yr_ago_val,
    CASE WHEN NVL(total_catg_sale_val, 0) != 0 THEN 100.0 * sale_val / total_catg_sale_val END AS sale_val_share_pct,
    CASE WHEN NVL(total_catg_sale_yr_ago_val, 0) != 0 THEN 100.0 * sale_yr_ago_val / total_catg_sale_yr_ago_val END AS sale_yr_ago_val_share_pct
  FROM tmfrm_catg_sale sale
);

-- Insert timeframe POS summary
DELETE FROM spr_tmfrm_sumry_final WHERE metrc_nm = 'POS_CATG_AMPS';
INSERT INTO spr_tmfrm_sumry_final (
    hier_type_cd,
    metrc_nm,
    tmfrm_cd,
    tmfrm_desc,
    corp_nm,
    sold_to_nbr,
    catg_nm,
    sale_val,
    sale_yr_ago_val,
    sale_val_share_pct,
    sale_yr_ago_val_share_pct,
    src_nm
  )
  SELECT
    'REX_COVERED' AS hier_type_cd,
    'POS_CATG_AMPS' AS metrc_nm,
    tmfrm_cd,
    CASE tmfrm_cd
      WHEN 'L4W' THEN 'Last 4 Weeks'
      WHEN 'L13W' THEN 'Last 13 Weeks'
      WHEN 'YTD' THEN 'Year to Date'
    END AS tmfrm_desc,
    corp_nm,
    sold_to_nbr,
    catg_nm,
    sale_val,
    sale_yr_ago_val,
    sale_val_share_pct,
    sale_yr_ago_val_share_pct,
    src_nm
  FROM pos_catg_tmfrm_sumry
;


----------------- POS Category SUMMARY END -----------------


/*****************************************************
*********  PERFORMANCE DRIVER SUMMARY START  *********
******************************************************/


-- get the latest visit and audit data
DROP TABLE IF EXISTS visit_audit_latst;
CREATE TEMP TABLE visit_audit_latst AS (
  -- SELECT * FROM (
    SELECT
      visit_guid,
      sold_to_nbr,
      mstr.chain_nbr,
      actl_start_dt_time,
      actn_desc,
      orchestro_item_alert_val,
      rep_item_alert_val,
      CASE WHEN (va.orchestro_item_alert_val IN ('Phantom','Zero Inventory','LowSales','Low Sales','Off Shelf','Phantom Alert','Neg Inventory','Innov No Sales', 'Distro Void')
        OR va.rep_item_alert_val = 1) THEN 1 END AS oos_alert_ind,
      prod_pack_extrnl_nbr,
      prod_pack_gtin,
      catg_nm,
      fisc_yr_wk,
      fisc_wk_end_dt,
      LOWER(va.src_nm) AS src_nm,
      va.kortex_upld_ts,
      va.kortex_dprct_ts,
      RANK() OVER (PARTITION BY sold_to_nbr, fisc_yr_wk ORDER BY actl_start_dt_time) AS visit_rank
    FROM sales_exec.retl_exec_store_visit_item_audit va
    INNER JOIN fin_acctg_ops.ref_fisc_cal_day ON fisc_dt = actl_start_dt_time::DATE
    INNER JOIN prod_hier_post_spin_matrl ON matrl_nbr = LPAD(prod_pack_extrnl_nbr, 18, '0')
    INNER JOIN spr_store_mstr mstr USING (sold_to_nbr)
    WHERE
      fisc_yr_wk BETWEEN (SELECT MIN(pd_start_yr_wk) FROM psd_pd_ref) AND (SELECT MAX(pd_end_yr_wk) FROM psd_pd_ref)
      AND (spin_ind = 1 OR klnva_retn_brand_ind = 1) AND hier_type_cd = 'REX_COVERED'
      AND catg_cd NOT IN ('2040') -- Fruit Snacks
  -- ) WHERE visit_rank = 1
);

RAISE INFO 'Creating OOS Timeframe Summary';
DROP TABLE IF EXISTS oos_tmfrm_sumry;
CREATE TEMP TABLE oos_tmfrm_sumry AS (
  WITH store_visits AS (
    SELECT
      sold_to_nbr,
      chain_nbr,
      catg_nm,
      tmfrm_cd,
      oos_alert_ind,
      visit_guid,
      src_nm,
      kortex_upld_ts,
      kortex_dprct_ts
    FROM visit_audit_latst
    INNER JOIN psd_pd_ref ON fisc_yr_wk BETWEEN pd_start_yr_wk AND pd_end_yr_wk
    WHERE TRUE
      -- AND visit_rank = 1 -- keep this filter for single visit/per week
  )
  SELECT
    sold_to_nbr,
    chain_nbr,
    catg_nm,
    tmfrm_cd,
    SUM(oos_alert_ind) AS oos_alert_cnt,
    MAX(visit_cnt) AS visit_cnt,
    src_nm,
    MAX(kortex_upld_ts) AS kortex_upld_ts,
    MAX(kortex_dprct_ts) AS kortex_dprct_ts
  FROM store_visits
  INNER JOIN (
    SELECT sold_to_nbr, tmfrm_cd, COUNT(DISTINCT visit_guid) AS visit_cnt FROM store_visits GROUP BY sold_to_nbr, tmfrm_cd
  ) USING (sold_to_nbr, tmfrm_cd)
  GROUP BY
    sold_to_nbr,
    chain_nbr,
    catg_nm,
    tmfrm_cd,
    src_nm
);

RAISE INFO 'Creating Events Timeframe Summary';
DROP TABLE IF EXISTS events_tmfrm_sumry;
CREATE TEMP TABLE events_tmfrm_sumry AS (
  WITH visit_latst AS (
    SELECT DISTINCT visit_guid, sold_to_nbr, fisc_yr_wk FROM visit_audit_latst WHERE visit_rank = 1
  ),
  visit_priorty AS (
    SELECT * --, ROW_NUMBER() OVER(PARTITION BY tmfrm_cd, sold_to_nbr, actl_yr_wk, mult_catg_nm) AS priorty_rn
    FROM (
    SELECT
      visit_guid,
      sold_to_nbr,
      chain_nbr,
      mult_catg_nm,
      pri.fisc_yr_wk AS actl_yr_wk,
      pri.priorty_nbr,
      up_in_store_ind,
      vst.fisc_yr_wk,
      tmfrm_cd,
      RANK() OVER (PARTITION BY sold_to_nbr, pri.fisc_yr_wk ORDER BY vst.fisc_yr_wk DESC) AS vst_pri_rank
    FROM sales_exec.retl_exec_store_priorty_stat pri
    INNER JOIN visit_latst vst USING (visit_guid, sold_to_nbr)
    INNER JOIN psd_pd_ref pd ON pri.fisc_yr_wk BETWEEN pd_start_yr_wk AND pd_end_yr_wk
    INNER JOIN spr_store_mstr cust USING (sold_to_nbr)
    WHERE pri.src_nm = 'sif_raw' AND hier_type_cd = 'REX_COVERED'
    AND UPPER(mult_catg_nm) NOT IN ('#REF!', '_XFFFF__XFFFF_','PWS','FROZEN','NEW','','CIRCULAR AD','CHECK FOR','FEATURE & DISPLAY','WINCO ORDER CODE','OTHER','#REF!','NULL')
    AND UPPER(mult_catg_nm) NOT LIKE '%CEREAL%'
    ) WHERE vst_pri_rank = 1
  ),
  priorty_wkly AS (
    SELECT
      'visit_weeks' AS wk_type_cd,
      pri.visit_guid,
      pri.sold_to_nbr,
      pri.chain_nbr,
      pri.mult_catg_nm,
      pri.actl_yr_wk,
      pri.priorty_nbr,
      pri.up_in_store_ind,
      tmfrm_cd
    FROM visit_priorty pri INNER JOIN visit_latst vst
      ON pri.sold_to_nbr = vst.sold_to_nbr AND pri.actl_yr_wk = vst.fisc_yr_wk
    UNION ALL
    SELECT
      'ans_weeks' AS wk_type_cd,
      pri.visit_guid,
      pri.sold_to_nbr,
      pri.chain_nbr,
      pri.mult_catg_nm,
      pri.actl_yr_wk,
      pri.priorty_nbr,
      pri.up_in_store_ind,
      tmfrm_cd
    FROM visit_priorty pri
    INNER JOIN (
      SELECT sold_to_nbr, actl_yr_wk, SUM(up_in_store_ind) AS up_in_store_val FROM visit_priorty GROUP BY sold_to_nbr, actl_yr_wk HAVING up_in_store_val > 0
    ) USING (sold_to_nbr, actl_yr_wk)
  ),
  priorty_tmfrm AS (
    SELECT *,
      ROW_NUMBER() OVER() AS priorty_rn  -- assign each row a unique number to avoid multiple counting once mult_catg_nm is split
    FROM (
      -- Rollup upinstore at store+catg
      SELECT tmfrm_cd, sold_to_nbr, chain_nbr, mult_catg_nm,
        SUM(CASE WHEN wk_type_cd = 'ans_weeks' THEN up_in_store_ind END) AS up_in_store_val,
        SUM(CASE WHEN wk_type_cd = 'ans_weeks' THEN 1 END) AS ans_in_store_val,
        SUM(CASE WHEN wk_type_cd = 'visit_weeks' THEN 1 END) AS visit_in_store_val
      FROM priorty_wkly
      GROUP BY tmfrm_cd, sold_to_nbr, chain_nbr, mult_catg_nm
    ) INNER JOIN (
      -- Rollup week count at store
      SELECT tmfrm_cd, sold_to_nbr,
        COUNT(DISTINCT CASE WHEN wk_type_cd = 'ans_weeks' THEN actl_yr_wk END) AS ans_wk_cnt,
        COUNT(DISTINCT CASE WHEN wk_type_cd = 'visit_weeks' THEN actl_yr_wk END) AS visit_wk_cnt,
        CASE WHEN ans_wk_cnt IS NOT NULL AND visit_wk_cnt IS NOT NULL
          THEN GREATEST(ans_wk_cnt, visit_wk_cnt)
        END AS true_in_store_wk_cnt
      FROM priorty_wkly
      GROUP BY sold_to_nbr, tmfrm_cd
    ) USING (tmfrm_cd, sold_to_nbr)
  ),
  tmp_split_string_iterator AS (
    SELECT 1 AS n UNION ALL
    SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
    SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
  )
  SELECT *,
    TRIM(SPLIT_PART(mult_catg_nm, ',', sep_i.n)) AS catg_nm
  FROM priorty_tmfrm
  CROSS JOIN tmp_split_string_iterator sep_i
  WHERE
    catg_nm IS NOT NULL AND catg_nm != ''
    AND UPPER(catg_nm) NOT IN ('#REF!', '_XFFFF__XFFFF_','PWS','FROZEN','NEW','','CIRCULAR AD','CHECK FOR','FEATURE & DISPLAY','WINCO ORDER CODE','OTHER','#REF!','NULL')
    AND UPPER(catg_nm) NOT LIKE '%CEREAL%'
);

RAISE INFO 'Creating Performance Driver Summary';

/*********************************************************************************************************
Creating this matrix to get Master List for Timeframe + Store/Chain + Individual/All/Event Categories
Definition: (Individual) -> All categories from Hierarchy
            (All) -> All categories with some activity in the store/chain
            (Event Categories) -> (All) minus Categories with activies for Priority
_________________________________________________________________________________________________
|               |                Store                  |                    Chain               |
| Metric        | Indv    | All      | Event            | Indv    | All            | Event       |
| Events        | Master  | Featured | Event<=>Featured | Master  | Store Event    | Store Event |
| OOS           | Master  | Featured | Event<=>Featured | Master  | Chain Featured | Store Event |
| POS_PRFM      | Master  | Featured | Event<=>Featured | Master  | Chain Featured | Store Event |
_________________________________________________________________________________________________
**********************************************************************************************************/
DROP TABLE IF EXISTS prfmnc_drvr_mstr_catg;
CREATE TEMP TABLE prfmnc_drvr_mstr_catg AS (
  WITH store_indv_categories AS (
    SELECT DISTINCT metrc_nm, tmfrm_cd, sold_to_nbr, chain_nbr, catg_nm
    FROM store_prod_mstr CROSS JOIN psd_pd_ref CROSS JOIN (
      SELECT METRC_EVENTS AS metrc_nm UNION SELECT METRC_OOS UNION SELECT METRC_POS_PRFM
    ) WHERE hier_type_cd = 'REX_COVERED'
  ),
  store_all_categories AS (
    SELECT DISTINCT mstr.* FROM store_indv_categories mstr
    LEFT JOIN (
      SELECT *, METRC_EVENTS AS metrc_nm FROM events_tmfrm_sumry WHERE ans_in_store_val > 0
    ) evt USING (metrc_nm, tmfrm_cd, sold_to_nbr, catg_nm)
    LEFT JOIN (
      SELECT *, METRC_OOS AS metrc_nm FROM oos_tmfrm_sumry
    ) oos USING (metrc_nm, tmfrm_cd, sold_to_nbr, catg_nm)
    LEFT JOIN (
      SELECT *, METRC_POS_PRFM AS metrc_nm FROM pos_tmfrm_sumry_without_ic
      WHERE corp_nm = POS_CORP_NM AND (sale_val+sale_yr_ago_val) > 0
    ) pos USING (metrc_nm, tmfrm_cd, sold_to_nbr, catg_nm)
    WHERE NVL(evt.metrc_nm, oos.metrc_nm, pos.metrc_nm) IS NOT NULL
  ),
  store_event_categories AS (
    SELECT DISTINCT sall.*
    FROM store_all_categories sall
    INNER JOIN store_all_categories evt USING (tmfrm_cd, sold_to_nbr, catg_nm)
    WHERE evt.metrc_nm = METRC_EVENTS
  ),
  store_categories AS (
    SELECT *,
      LISTAGG(DISTINCT catg_nm, ', ') WITHIN GROUP (ORDER BY catg_nm) OVER(PARTITION BY
        CASE WHEN catg_type_cd = METRC_INDV_CATG THEN catg_nm ELSE catg_type_cd END,
        metrc_nm, tmfrm_cd, sold_to_nbr) AS mult_catg_nm
    FROM (
      SELECT METRC_INDV_CATG AS catg_type_cd, * FROM store_indv_categories UNION ALL
      SELECT METRC_ALL_CATG AS catg_type_cd, * FROM store_all_categories UNION ALL
      SELECT METRC_EVENT_CATG AS catg_type_cd, * FROM store_event_categories
    )
  ),
  chain_categories AS (
    SELECT str.*, catg_type_cd, chain_mult_catg_nm -- what categories from this store are useful in chain agg
    FROM (
      SELECT DISTINCT catg_type_cd, metrc_nm, tmfrm_cd, chain_nbr, catg_nm, mult_catg_nm AS chain_mult_catg_nm
      FROM store_categories WHERE catg_type_cd != METRC_ALL_CATG OR (catg_type_cd = METRC_ALL_CATG AND metrc_nm = METRC_EVENTS)
      UNION ALL
      SELECT DISTINCT catg_type_cd, metrc_nm, tmfrm_cd, chain_nbr, catg_nm, 
        LISTAGG(DISTINCT catg_nm, ', ') WITHIN GROUP (ORDER BY catg_nm)
          OVER(PARTITION BY metrc_nm, tmfrm_cd, chain_nbr) AS chain_mult_catg_nm
      FROM store_categories WHERE catg_type_cd = METRC_ALL_CATG AND metrc_nm != METRC_EVENTS
    ) INNER JOIN store_all_categories str USING (metrc_nm, tmfrm_cd, chain_nbr, catg_nm)
  )
  SELECT 'store' AS store_chain_ind, metrc_nm, tmfrm_cd, sold_to_nbr, chain_nbr, catg_nm, mult_catg_nm, catg_type_cd FROM store_categories
  UNION ALL
  SELECT 'chain' AS store_chain_ind, metrc_nm, tmfrm_cd, sold_to_nbr, chain_nbr, catg_nm, chain_mult_catg_nm, catg_type_cd FROM chain_categories
);

-- Utilizing the master list to create performance drivers summary
DROP TABLE IF EXISTS prfm_drvr_sumry_pos_prfm_oos_events;
CREATE TEMP TABLE prfm_drvr_sumry_pos_prfm_oos_events AS (
  WITH store_catg_facts_events AS (
    SELECT
      mstr.metrc_nm,
      mstr.tmfrm_cd,
      mstr.sold_to_nbr,
      mstr.chain_nbr,
      mstr.store_chain_ind,
      CASE WHEN catg_type_cd = '(Individual)' THEN mstr.catg_nm ELSE mstr.catg_type_cd END AS catg_nm,
      mstr.mult_catg_nm,
      fct.priorty_rn,
      MAX(up_in_store_val) AS up_in_store_val,
      MAX(ans_in_store_val) AS ans_in_store_val,
      MAX(visit_in_store_val) AS visit_in_store_val,
      MAX(true_in_store_wk_cnt) AS true_in_store_wk_cnt
    FROM prfmnc_drvr_mstr_catg mstr
    LEFT JOIN events_tmfrm_sumry fct USING (tmfrm_cd, catg_nm, sold_to_nbr)
    WHERE mstr.metrc_nm = METRC_EVENTS
    GROUP BY
      mstr.metrc_nm,
      mstr.tmfrm_cd,
      mstr.sold_to_nbr,
      mstr.chain_nbr,
      mstr.store_chain_ind,
      CASE WHEN catg_type_cd = '(Individual)' THEN mstr.catg_nm ELSE mstr.catg_type_cd END,
      mstr.mult_catg_nm,
      fct.priorty_rn
  ),
  store_catg_facts AS (
    SELECT
      metrc_nm,
      tmfrm_cd,
      store_chain_ind,
      sold_to_nbr,
      chain_nbr,
      catg_nm,
      mult_catg_nm,
    -- priority facts
      SUM(up_in_store_val) AS up_in_store_val,
      SUM(ans_in_store_val) AS rollup_ans_in_store_val,
      SUM(visit_in_store_val) AS rollup_visit_in_store_val,
      CASE
        WHEN rollup_ans_in_store_val IS NOT NULL AND rollup_visit_in_store_val IS NOT NULL THEN
          GREATEST(rollup_ans_in_store_val, rollup_visit_in_store_val)
      END AS true_in_store_val,
      MAX(true_in_store_wk_cnt) AS true_in_store_wk_cnt,
    -- pos facts
      NULL::FLOAT8 AS sale_val,
      NULL::FLOAT8 AS sale_yr_ago_val,
      NULL::FLOAT8 AS sale_qty,
      NULL::FLOAT8 AS sale_yr_ago_qty,
      NULL::FLOAT8 AS sale_vol_lb_val,
      NULL::FLOAT8 AS sale_yr_ago_vol_lb_val,
    -- oos facts
      NULL::FLOAT8 AS oos_alert_cnt,
      NULL::FLOAT8 AS visit_cnt
    FROM store_catg_facts_events
    GROUP BY
      metrc_nm,
      tmfrm_cd,
      store_chain_ind,
      sold_to_nbr,
      chain_nbr,
      catg_nm,
      mult_catg_nm
    UNION ALL
    SELECT
      mstr.metrc_nm,
      mstr.tmfrm_cd,
      store_chain_ind,
      CASE store_chain_ind
        WHEN 'store' THEN mstr.sold_to_nbr
        WHEN 'chain' THEN ''  -- no need to track store for chain level aggregation
      END AS sold_to_nbr,
      mstr.chain_nbr,
      CASE
        WHEN catg_type_cd = '(Individual)' THEN mstr.catg_nm
        ELSE catg_type_cd     -- no need to track individual categories for (All) and (Event Categories)
      END AS catg_nm,
      mstr.mult_catg_nm,
    -- priority facts
      NULL::FLOAT8 AS up_in_store_val,
      NULL::FLOAT8 AS rollup_ans_in_store_val,
      NULL::FLOAT8 AS rollup_visit_in_store_val,
      NULL::FLOAT8 AS true_in_store_val,
      NULL::FLOAT8 AS true_in_store_wk_cnt,
    -- pos facts
      SUM(sale_val) AS sale_val,
      SUM(sale_yr_ago_val) AS sale_yr_ago_val,
      SUM(sale_qty) AS sale_qty,
      SUM(sale_yr_ago_qty) AS sale_yr_ago_qty,
      SUM(sale_vol_lb_val) AS sale_vol_lb_val,
      SUM(sale_yr_ago_vol_lb_val) AS sale_yr_ago_vol_lb_val,
    -- oos facts
      SUM(oos_alert_cnt) AS oos_alert_cnt,
      MAX(visit_cnt) AS visit_cnt
    FROM prfmnc_drvr_mstr_catg mstr
    LEFT JOIN (SELECT *, METRC_OOS AS metrc_nm FROM oos_tmfrm_sumry) oos USING (metrc_nm, tmfrm_cd, catg_nm, sold_to_nbr)
    LEFT JOIN (
      SELECT *, METRC_POS_PRFM AS metrc_nm FROM pos_tmfrm_sumry_without_ic
      WHERE corp_nm = POS_CORP_NM AND tmfrm_cd NOT LIKE 'WEEKLY%'
    ) pos USING (metrc_nm, tmfrm_cd, catg_nm, sold_to_nbr)
    WHERE mstr.metrc_nm != METRC_EVENTS
    GROUP BY
      mstr.metrc_nm,
      mstr.tmfrm_cd,
      store_chain_ind,
      CASE store_chain_ind
        WHEN 'store' THEN mstr.sold_to_nbr
        WHEN 'chain' THEN ''
      END,
      mstr.chain_nbr,
      CASE
        WHEN catg_type_cd = '(Individual)' THEN mstr.catg_nm
        ELSE catg_type_cd
      END,
      mstr.mult_catg_nm
  ),
  chain_catg_facts AS (
    SELECT metrc_nm, tmfrm_cd, chain_nbr, catg_nm, mult_catg_nm,
      SUM(up_in_store_val) AS chain_up_in_store_val,
      SUM(rollup_ans_in_store_val) AS chain_ans_in_store_val,
      SUM(true_in_store_val) AS chain_true_in_store_val,
      NULL::FLOAT8 AS chain_sale_val,
      NULL::FLOAT8 AS chain_sale_yr_ago_val,
      NULL::FLOAT8 AS chain_sale_qty,
      NULL::FLOAT8 AS chain_sale_yr_ago_qty,
      NULL::FLOAT8 AS chain_sale_vol_lb_val,
      NULL::FLOAT8 AS chain_sale_yr_ago_vol_lb_val,
      NULL::FLOAT8 AS chain_oos_alert_cnt
    FROM store_catg_facts
    WHERE store_chain_ind = 'chain' AND metrc_nm = METRC_EVENTS
    GROUP BY metrc_nm, tmfrm_cd, chain_nbr, catg_nm, mult_catg_nm
    UNION ALL
    SELECT metrc_nm, tmfrm_cd, chain_nbr, catg_nm, mult_catg_nm,
      NULL::FLOAT8 AS chain_up_in_store_val,
      NULL::FLOAT8 AS chain_ans_in_store_val,
      NULL::FLOAT8 AS chain_true_in_store_val,
      sale_val AS chain_sale_val,
      sale_yr_ago_val AS chain_sale_yr_ago_val,
      sale_qty AS chain_sale_qty,
      sale_yr_ago_qty AS chain_sale_yr_ago_qty,
      sale_vol_lb_val AS chain_sale_vol_lb_val,
      sale_yr_ago_vol_lb_val AS chain_sale_yr_ago_vol_lb_val,
      oos_alert_cnt AS chain_oos_alert_cnt
    FROM store_catg_facts
    WHERE store_chain_ind = 'chain' AND metrc_nm != METRC_EVENTS
  ),
  store_lvl_facts AS (
    SELECT *,
      RANK() OVER (
        PARTITION BY metrc_nm, tmfrm_cd, chain_nbr
        ORDER BY CASE metrc_nm
          WHEN METRC_EVENTS THEN events_rank_critra  -- desc
          WHEN METRC_OOS THEN NVL(oos_rank_critra, 0)  -- asc
          WHEN METRC_POS_PRFM THEN pos_rank_critra  -- desc
      END) AS prfmnc_drvr_rank
    FROM (
      SELECT metrc_nm, tmfrm_cd, sold_to_nbr, chain_nbr,
        sale_val AS store_sale_val,
        sale_yr_ago_val AS store_sale_yr_ago_val,
        oos_alert_cnt AS store_oos_alert_cnt,
        visit_cnt AS store_visit_cnt,
        up_in_store_val AS store_up_in_store_val,
        true_in_store_val AS store_true_in_store_val,
        true_in_store_wk_cnt AS store_true_in_store_wk_cnt,
        CASE WHEN NVL(store_true_in_store_val, 0) = 0 THEN 0
          ELSE -1.0 * store_up_in_store_val / store_true_in_store_val END AS events_rank_critra,
        CASE WHEN NVL(store_visit_cnt,0) = 0 THEN 0
          ELSE 1.0 * store_oos_alert_cnt / store_visit_cnt END AS oos_rank_critra,
        CASE WHEN store_sale_yr_ago_val = 0 THEN 0
          ELSE -100.0 * (store_sale_val - store_sale_yr_ago_val) / store_sale_yr_ago_val END AS pos_rank_critra
      FROM store_catg_facts
      WHERE catg_nm = METRC_ALL_CATG AND store_chain_ind = 'store'
    )
  ),
  chain_lvl_facts AS (
    SELECT metrc_nm, tmfrm_cd, chain_nbr,
      COUNT(DISTINCT sold_to_nbr) AS chain_store_cnt,
      SUM(store_visit_cnt) AS chain_visit_cnt,
      -- SUM(store_true_in_store_val) AS chain_true_in_store_val,
      SUM(store_true_in_store_wk_cnt) AS chain_true_in_store_wk_cnt
    FROM store_lvl_facts
    GROUP BY metrc_nm, tmfrm_cd, chain_nbr
  )
  SELECT
    -- store catg facts
    str_catg.*,
    rollup_ans_in_store_val AS ans_in_store_val,
    -- chain catg facts
    chn_catg.chain_up_in_store_val,
    chn_catg.chain_ans_in_store_val,
    chn_catg.chain_true_in_store_val,
    chn_catg.chain_sale_val,
    chn_catg.chain_sale_yr_ago_val,
    chn_catg.chain_sale_qty,
    chn_catg.chain_sale_yr_ago_qty,
    chn_catg.chain_sale_vol_lb_val,
    chn_catg.chain_sale_yr_ago_vol_lb_val,
    chn_catg.chain_oos_alert_cnt,
    -- store facts
    str.store_sale_val,
    str.store_sale_yr_ago_val,
    str.store_oos_alert_cnt,
    str.store_visit_cnt,
    str.store_up_in_store_val,
    str.store_true_in_store_val,
    str.store_true_in_store_wk_cnt,
    str.events_rank_critra,
    str.oos_rank_critra,
    str.pos_rank_critra,
    str.prfmnc_drvr_rank,
    -- chain facts
    chn.chain_store_cnt,
    chn.chain_visit_cnt,
    chn.chain_true_in_store_wk_cnt
  FROM (SELECT * FROM store_catg_facts WHERE store_chain_ind = 'store') str_catg
  LEFT JOIN chain_catg_facts chn_catg USING (metrc_nm, tmfrm_cd, chain_nbr, catg_nm)
  LEFT JOIN store_lvl_facts str USING (metrc_nm, tmfrm_cd, sold_to_nbr, chain_nbr)
  LEFT JOIN chain_lvl_facts chn USING (metrc_nm, tmfrm_cd, chain_nbr)
  WHERE
    CASE
      WHEN chn_catg.catg_nm = METRC_EVENT_CATG OR chn_catg.metrc_nm = METRC_EVENTS
        THEN chn_catg.mult_catg_nm = str_catg.mult_catg_nm
    ELSE TRUE END
);


-- EVENTS, OOS, POS_PRFM
DELETE FROM spr_tmfrm_sumry_final WHERE metrc_nm IN (METRC_EVENTS, METRC_OOS, METRC_POS_PRFM);
INSERT INTO spr_tmfrm_sumry_final (
  metrc_nm,
  tmfrm_cd,
  sold_to_nbr,
  chain_nbr,
  catg_nm,
  mult_catg_nm,
  up_in_store_val,
  ans_in_store_val,
  true_in_store_val,
  true_in_store_wk_cnt,
  chain_up_in_store_val,
  chain_ans_in_store_val,
  chain_true_in_store_val,
  chain_true_in_store_wk_cnt,
  sale_val,
  sale_yr_ago_val,
  sale_qty,
  sale_yr_ago_qty,
  chain_sale_val,
  chain_sale_yr_ago_val,
  chain_sale_qty,
  chain_sale_yr_ago_qty,
  oos_alert_cnt,
  visit_cnt,
  chain_oos_alert_cnt,
  chain_visit_cnt,
  chain_store_cnt,
  prfmnc_drvr_rank,
  src_nm,
  kortex_upld_ts,
  kortex_dprct_ts
  )
  SELECT
    metrc_nm,
    tmfrm_cd,
    sold_to_nbr,
    chain_nbr,
    catg_nm,
    mult_catg_nm,
    -- priority facts
    up_in_store_val,
    ans_in_store_val,
    true_in_store_val,
    store_true_in_store_wk_cnt AS true_in_store_wk_cnt,
    chain_up_in_store_val,
    chain_ans_in_store_val,
    chain_true_in_store_val,
    chain_true_in_store_wk_cnt,
    -- pos facts
    sale_val,
    sale_yr_ago_val,
    sale_qty,
    sale_yr_ago_qty,
    chain_sale_val,
    chain_sale_yr_ago_val,
    chain_sale_qty,
    chain_sale_yr_ago_qty,
    -- oos facts
    oos_alert_cnt,
    store_visit_cnt AS visit_cnt,
    chain_oos_alert_cnt,
    chain_visit_cnt,
    chain_store_cnt,
    prfmnc_drvr_rank,
    'kna_ecc' AS src_nm,
    NULL::TIMESTAMP AS kortex_upld_ts,
    NULL::TIMESTAMP AS kortex_dprct_ts
  FROM prfm_drvr_sumry_pos_prfm_oos_events
;

-- DROP TABLE IF EXISTS prfm_drvr_sumry_pos_prfm_oos_events;

------------- PERFORMANCE DRIVER SUMMARY END -------------

-- Deleting categories with no data
DELETE FROM spr_tmfrm_sumry_final WHERE catg_nm IN (
  SELECT catg_nm FROM (
    SELECT
      catg_nm,
      NVL(SUM(sale_val), 0) AS total_sale_val,
      NVL(SUM(true_in_store_val), 0) AS total_true_in_store_val,
      NVL(SUM(oos_alert_cnt), 0) AS total_oos_alert_cnt
    FROM spr_tmfrm_sumry_final
    GROUP BY catg_nm
  )
  WHERE NOT (
    total_sale_val > 0 OR
    total_true_in_store_val > 0 OR
    total_oos_alert_cnt > 0
  )
);


RAISE INFO 'Inserting Master list for location (rgn/zone/terr/plan_to etc)';

-- Insert master list for region/zone/terr/plan_to
DELETE FROM spr_tmfrm_sumry_final WHERE sold_to_nbr = '';
INSERT INTO spr_tmfrm_sumry_final (
  hier_type_cd,
  metrc_nm,
  chain_nbr,
  chain_nm,
  rgn_nbr,
  rgn_nm,
  zn_nbr,
  zn_nm,
  terr_nbr,
  terr_nm,
  plan_to_nbr,
  plan_to_nm,
  sales_org_cd,
  corp_nm,
  sold_to_nbr,
  chain_store_cnt,
  src_nm
  )
  SELECT DISTINCT
    hier_type_cd,
    'POS' AS metrc_nm,
    chain_nbr,
    chain_nm,
    rgn_nbr,
    rgn_nm,
    zn_nbr,
    zn_nm,
    terr_nbr,
    terr_nm,
    plan_to_nbr,
    plan_to_nm,
    sales_org_cd,
    POS_CORP_NM AS corp_nm,
    '' AS sold_to_nbr,
    chain_store_cnt,
    src_nm
  FROM spr_store_mstr
  INNER JOIN (
    SELECT
      chain_nbr,
      COUNT(DISTINCT sold_to_nbr) AS chain_store_cnt
    FROM spr_store_mstr
    WHERE hier_type_cd = 'REX_COVERED'
    GROUP BY
      chain_nbr
  ) USING (chain_nbr)
  WHERE hier_type_cd = 'REX_COVERED'
  UNION ALL
  SELECT DISTINCT
    hier_type_cd,
    'POS' AS metrc_nm,
    NULL AS chain_nbr,
    NULL AS chain_nm,
    rgn_nbr,
    rgn_nm,
    zn_nbr,
    zn_nm,
    terr_nbr,
    terr_nm,
    NULL AS plan_to_nbr,
    NULL AS plan_to_nm,
    sales_org_cd,
    POS_CORP_NM AS corp_nm,
    NULL AS sold_to_nbr,
    NULL::INTEGER AS chain_store_cnt,
    src_nm
  FROM spr_store_mstr
  WHERE hier_type_cd = 'REX_NON_COVERED'
  UNION ALL
  SELECT DISTINCT
    hier_type_cd,
    'POS' AS metrc_nm,
    NULL AS chain_nbr,
    NULL AS chain_nm,
    rgn_nbr,
    rgn_nm,
    zn_nbr,
    zn_nm,
    terr_nbr,
    terr_nm,
    plan_to_nbr,
    plan_to_nm,
    sales_org_cd,
    POS_CORP_NM AS corp_nm,
    NULL AS sold_to_nbr,
    NULL::INTEGER AS chain_store_cnt,
    src_nm
  FROM spr_store_mstr
  WHERE hier_type_cd = 'REX_NON_COVERED'
  UNION ALL
  SELECT DISTINCT
    hier_type_cd,
    'POS' AS metrc_nm,
    chain_nbr,
    chain_nm,
    rgn_nbr,
    rgn_nm,
    zn_nbr,
    zn_nm,
    terr_nbr,
    terr_nm,
    plan_to_nbr,
    plan_to_nm,
    sales_org_cd,
    POS_CORP_NM AS corp_nm,
    NULL AS sold_to_nbr,
    NULL::INTEGER AS chain_store_cnt,
    src_nm
  FROM spr_store_mstr
  WHERE hier_type_cd = 'REX_NON_COVERED'
;

RAISE INFO 'Updating Store details from master hierarchy';
-- Update details from Customer Hierarchy
UPDATE spr_tmfrm_sumry_final dm
SET
  rgn_nm = mstr.rgn_nm,
  rgn_nbr = mstr.rgn_nbr,
  zn_nm = mstr.zn_nm,
  zn_nbr = mstr.zn_nbr,
  terr_nm = mstr.terr_nm,
  terr_nbr = mstr.terr_nbr,
  chnl_nm = mstr.chnl_nm,
  chnl_nbr = mstr.chnl_nbr,
  plan_to_nm = mstr.plan_to_nm,
  plan_to_nbr = mstr.plan_to_nbr,
  chain_nm = mstr.chain_nm,
  chain_nbr = mstr.chain_nbr,
  sales_org_cd = mstr.sales_org_cd,
  sold_to_nm = mstr.sold_to_nm,
  sold_to_nbr = mstr.sold_to_nbr,
  sold_to_desc = mstr.sold_to_desc,
  tdlinx_nbr = mstr.tdlinx_nbr,
  store_nm = mstr.store_nm,
  street_nm = mstr.street_nm,
  city_nm = mstr.city_nm,
  rgn_cd = mstr.rgn_cd,
  pstl_cd = mstr.pstl_cd,
  store_nbr = mstr.store_nbr,
  prev_acct_nbr = mstr.prev_acct_nbr,
  vndr_nm = mstr.vndr_nm
FROM spr_store_mstr mstr
WHERE
  mstr.sold_to_nbr = dm.sold_to_nbr
;

RAISE INFO 'Persisting into datamart';
-- Persist into the datamart
DELETE FROM sales_exec.dm_retl_exec_store_prfmnc_tmfrm_sumry WHERE TRUE;
INSERT INTO sales_exec.dm_retl_exec_store_prfmnc_tmfrm_sumry (
  SELECT
    NVL(hier_type_cd, 'REX_COVERED') AS hier_type_cd,
    NVL(metrc_nm, '') AS metrc_nm,
    NVL(sub_metrc_nm, '') AS sub_metrc_nm,
    NVL(tmfrm_cd, '') AS tmfrm_cd,
    NVL(tmfrm_desc, '') AS tmfrm_desc,
    NVL(fisc_yr, 0) AS fisc_yr,
    NVL(sold_to_nbr, '') AS sold_to_nbr,
    NVL(chain_nbr, '') AS chain_nbr,
    NVL(chain_nm, '') AS chain_nm,
    NVL(plan_to_nm, '') AS plan_to_nm,
    NVL(plan_to_nbr, '') AS plan_to_nbr,
    NVL(sales_org_cd, '') AS sales_org_cd,
    NVL(catg_nm, '') AS catg_nm,
    NVL(mult_catg_nm, '') AS mult_catg_nm,
    NVL(brand_nm, '') AS brand_nm,
    NVL(corp_nm, POS_CORP_NM) AS corp_nm,
    sale_val,
    sale_yr_ago_val,
    sale_qty,
    sale_yr_ago_qty,
    sale_val_share_pct,
    sale_yr_ago_val_share_pct,
    store_sale_val,
    store_sale_yr_ago_val,
    store_sale_qty,
    store_sale_yr_ago_qty,
    store_sale_vs_yr_ago_val_pct,
    store_sale_vs_yr_ago_qty_pct,
    chain_sale_val,
    chain_sale_yr_ago_val,
    chain_sale_qty,
    chain_sale_yr_ago_qty,
    chain_sale_vs_yr_ago_val_pct,
    chain_sale_vs_yr_ago_qty_pct,
    chain_store_sale_val_rank,
    chain_store_sale_qty_rank,
    brand_sale_rank,
    -- allowed_brand_rank,
    sale_vs_yr_ago_val,
    sale_vs_yr_ago_val_pct,
    prfmnc_drvr_rank,
    chain_store_cnt,
    up_in_store_val,
    ans_in_store_val,
    visit_in_store_val,
    true_in_store_val,
    true_in_store_wk_cnt,
    chain_up_in_store_val,
    chain_ans_in_store_val,
    chain_visit_in_store_val,
    chain_true_in_store_val,
    chain_true_in_store_wk_cnt,
    visit_cnt,
    oos_alert_cnt,
    chain_visit_cnt,
    chain_oos_alert_cnt,
    NVL(sold_to_nm, '') AS sold_to_nm,
    NVL(sold_to_desc, '') AS sold_to_desc,
    CASE WHEN hier_type_cd = 'REX_NON_COVERED' AND NVL(terr_nbr, '') = '' THEN 'RETAIL NON-COVERED STORES' ELSE NVL(rgn_nm, '') END AS rgn_nm,
    CASE WHEN hier_type_cd = 'REX_NON_COVERED' AND NVL(terr_nbr, '') = '' THEN 'RETAIL NON-COVERED STORES' ELSE NVL(rgn_nbr, '') END AS rgn_nbr,
    CASE WHEN hier_type_cd = 'REX_NON_COVERED' AND NVL(terr_nbr, '') = '' THEN 'RETAIL NON-COVERED STORES' ELSE NVL(zn_nm, '') END AS zn_nm,
    CASE WHEN hier_type_cd = 'REX_NON_COVERED' AND NVL(terr_nbr, '') = '' THEN 'RETAIL NON-COVERED STORES' ELSE NVL(zn_nbr, '') END AS zn_nbr,
    CASE WHEN hier_type_cd = 'REX_NON_COVERED' AND NVL(terr_nbr, '') = '' THEN 'RETAIL NON-COVERED STORES' ELSE NVL(terr_nm, '') END AS terr_nm,
    CASE WHEN hier_type_cd = 'REX_NON_COVERED' AND NVL(terr_nbr, '') = '' THEN 'RETAIL NON-COVERED STORES' ELSE NVL(terr_nbr, '') END AS terr_nbr,
    NVL(chnl_nm, '') AS chnl_nm,
    NVL(chnl_nbr, '') AS chnl_nbr,
    NVL(tdlinx_nbr, '') AS tdlinx_nbr,
    NVL(store_nm, '') AS store_nm,
    NVL(street_nm, '') AS street_nm,
    NVL(city_nm, '') AS city_nm,
    NVL(rgn_cd, '') AS rgn_cd,
    NVL(pstl_cd, '') AS pstl_cd,
    NVL(store_nbr, '') AS store_nbr,
    NVL(prev_acct_nbr, '') AS prev_acct_nbr,
    NVL(vndr_nm, '') AS vndr_nm,
    'dol' AS val_curr_cd,
    'each' AS qty_uom,
    pos_run_dt AS latst_data_avail_dt,
    NVL(src_nm, 'kna_ecc') AS src_nm,
    Md5(src_nm || metrc_nm || sub_metrc_nm || tmfrm_cd || tmfrm_desc || fisc_yr || sold_to_nbr || catg_nm || mult_catg_nm || brand_nm || corp_nm) AS hash_key,
    kortex_upld_ts,
    kortex_dprct_ts,
    CURRENT_TIMESTAMP AS kortex_cre_ts,
    CURRENT_TIMESTAMP AS kortex_updt_ts
  FROM spr_tmfrm_sumry_final
);


-- Raise the error info in case of a failure because of any exception
EXCEPTION WHEN OTHERS THEN 
    RAISE INFO 'An exception occurred in sales_exec.dm_retl_exec_store_prfmnc_tmfrm_sumry.';
    RAISE INFO 'Error code: %, Error message: %', SQLSTATE, SQLERRM;


COMMIT;

END;
