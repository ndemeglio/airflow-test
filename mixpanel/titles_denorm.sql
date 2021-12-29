-- view created in `acv-data.denorms.titles`
SELECT
      a.id as auction_id
    , title.max_seller_payment_dt_est
    , title.max_title_sent_dt_est
    , title.max_title_recd_dt_est
    , title_status.title_state
    , title_status.seller_payment_date
    , IF(COALESCE(cra.yes_no, FALSE),
      1,
      0) AS title_attached
    , IF(COALESCE(ttl_probs.value, FALSE),
      1,
      0) AS title_problem_ever
FROM
  `acv-auctions-eng-global.acv_prod.auctions` a
JOIN (
    SELECT
      auction_id,
      MAX(DATETIME(seller_payment_date, 'America/New_York')) AS max_seller_payment_dt_est,
      MAX(DATETIME(title_sent_date, 'America/New_York')) AS max_title_sent_dt_est,
      MAX(DATETIME(title_received_date, 'America/New_York')) AS max_title_recd_dt_est
    FROM
      `acv-auctions-eng-global.acv_prod.title`
    WHERE deleted = FALSE
    GROUP BY auction_id
  ) title ON title.auction_id = a.id
LEFT JOIN (
      SELECT
          t.auction_id,
          t.title_state,
          t.seller_payment_date

      FROM `acv-auctions-eng-global.acv_prod.title` t
      INNER JOIN(
                  SELECT
                      auction_id,
                      MAX(id) as max_id
                  FROM `acv-auctions-eng-global.acv_prod.title`
                  WHERE unwound = False
                      AND deleted = False
                  GROUP BY auction_id
                  ) sq ON t.id = sq.max_id
      ) title_status ON a.id = title_status.auction_id
LEFT JOIN (
  SELECT *
    from `acv-auctions-eng-global.acv_prod.condition_report_answers`
    where question_id = 78
  ) cra on cra.auction_id = a.id

LEFT JOIN (
  SELECT
    a.id AS auction_id,
    TRUE AS value
  FROM
    `acv-auctions-eng-global.acv_prod.auctions` a
  LEFT JOIN
    `acv-auctions-eng-global.acv_prod.title` t
  ON
    a.id = t.auction_id
  LEFT JOIN
    `acv-auctions-eng-global.acv_prod.title_note` tn
  ON
    t.id = tn.title_id
  WHERE
    (tn.text LIKE 'Status changed to:%'
      AND SUBSTR(tn.text, 20) IN ('Returned to Seller',
        'Problem',
        'Buyer Problem',
        '48 Hour Notice',
        'Returned to Seller Unwound') )
    OR t.title_state IN ('problem',
      'bp',
      'rts',
      'unwound',
      'hr48',
      'at_acv_unwound')
  GROUP BY
    a.id) ttl_probs
ON
  ttl_probs.auction_id = a.id
