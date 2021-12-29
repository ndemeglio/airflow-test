-- view created at `acv-data.denorms.resolutions`
SELECT
    a.id as auction_id
  , void.unwound_dt_est
  , void.notes as unwound_notes
  , relist.relist_dt_est
FROM
  `acv-auctions-eng-global.acv_prod.auctions` a
JOIN (
  SELECT
    astat.auction_id,
    astat.notes,
    DATETIME(astat.time,
      'America/New_York') AS unwound_dt_est
  FROM
    `acv-auctions-eng-global.acv_prod.auction_status` astat
  INNER JOIN (
    SELECT
      auction_id,
      MIN(id) AS min_id
    FROM
      `acv-auctions-eng-global.acv_prod.auction_status`
    WHERE
      status_name LIKE '%unwound'
    GROUP BY
      auction_id ) end_date_id
  ON
    astat.id = end_date_id.min_id ) void
ON
  a.id = void.auction_id
LEFT JOIN (
  SELECT
    a.vin,
    MIN(DATETIME(a.end_time,
        'America/New_York')) AS relist_dt_est
  FROM
    `acv-auctions-eng-global.acv_prod.auctions` a
  WHERE
    a.dealer_id  = 4308
  GROUP BY
    a.vin ) relist
ON
  a.vin = relist.vin
