-- view created in `acv-data.denorms.base_denorm`
SELECT
  a.auction_id
  , a.s_dealer_id
  , pmt.b_dealer_id
  , a.auction_year
  , a.auction_month
  , a.auction_week
  , a.auction_end_dt_est
  , a.sold
  , a.unwound
  , a.status
  , a.vin
  , a.green_light
  , a.auction_start_dt_est
  , a.seller_dealership_name
  , a.auction_status_updated_time
  , index.work_day_value
  , index.cumulative_work_days
  , ttl.max_seller_payment_dt_est
  , ttl.max_title_sent_dt_est
  , ttl.max_title_recd_dt_est
  , ttl.title_attached
  , ttl.title_problem_ever
  , ttl.title_state
  , ttl.seller_payment_date
  , pmt.current_payment_dt_est
  , pmt.current_payment_status
  , pmt.current_payment_method
  , a.arbitration_id
  , auc_adj.adj_acv_check_amount
  , auc_adj.adj_acv_credit_amount
  , auc_adj.adj_acv_bought_amount
  , auc_adj.any_valid
  , auc_adj.perc_valid
  , auc_adj.any_avoidable
  , auc_adj.perc_avoidable
  , auc_adj.is_open
  , auc_adj.adj_total_amount
  , auc_adj.avoidable_adj_amount
  , auc_adj.arb_open_dt_est
  , auc_adj.arb_close_dt_est
  , auc_adj.any_goodwill
  , auc_adj.goodwill_amount
  , auc_adj.acv_purchased_adj_amount
  , res.unwound_dt_est
  , res.relist_dt_est
  , res.unwound_notes
  , trans.requested_dt_est
  , trans.assigned_dt_est
  , trans.delivered_dt_est
  , trans.price_charged_to_customer
  , trans.price_paid
  , trans.picked_up_dt_est
  , trans.created_dt_est
  , trans.posted_dt_est
  , trans.distance
  , budget_sales.budget_unit_sales
FROM
  `acv-data.denorms.auctions` a
-- CALENDAR
LEFT JOIN (
  SELECT
    *
  FROM
    `acv-auctions-eng-global.acv_finance.working_day_counts`) index
ON
  index.date = DATE(a.end_time, 'America/New_York')

-- Budget Unit Sales
LEFT JOIN (
   SELECT
     *
    FROM
     `acv-auctions-eng-global.acv_finance.kpi_monthly_values`) budget_sales
ON budget_sales.yr = a.auction_year and budget_sales.mo = a.auction_month

-- TITLES
LEFT JOIN `acv-data.denorms.titles` ttl on ttl.auction_id = a.auction_id

-- PAYMENTS
LEFT JOIN `acv-data.denorms.payments` pmt on pmt.auction_id = a.auction_id

 -- ARBITRATIONS
LEFT JOIN `acv-data.denorms.arbs` auc_adj on auc_adj.auction_id =a.auction_id

-- RESOLUTION
LEFT JOIN `acv-data.denorms.resolutions` res on res.auction_id = a.auction_id

-- TRANSPORTATION
LEFT JOIN `acv-data.denorms.transportation` trans on trans.auction_id = a.auction_id
