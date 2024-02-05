DROP TABLE IF EXISTS mart.f_customer_retention;
CREATE TABLE mart.f_customer_retention(
      id                            SERIAL8                  --идентификатор записи
    , new_customers_count           INT4            NOT NULL --кол-во новых клиентов
    , returning_customers_count     INT4            NOT NULL --кол-во вернувшихся клиентов
    , refunded_customer_count       INT4            NOT NULL --кол-во клиентов, оформивших возврат
    , period_name                   VARCHAR(100)    NOT NULL --рассматирваемый период
    , period_id                     INT4            NOT NULL --идентификатор периода (номер недели или номер месяца)
    , item_id                       INT8            NOT NULL --идентификатор категории товара
    , new_customers_revenue         NUMERIC(19,2)   NOT NULL --доход с новых клиентов
    , returning_customers_revenue   NUMERIC(19,2)   NOT NULL --доход с вернувшихся клиентов
    , customers_refunded            INT4            NOT NULL --количество возвратов клиентов
    
    , CONSTRAINT pk_f_customer_retention_id PRIMARY KEY(id)
);

COMMENT ON TABLE mart.f_customer_retention IS 'Возвращаемость клиентов';

COMMENT ON COLUMN mart.f_customer_retention.id IS 'идентификатор записи';
COMMENT ON COLUMN mart.f_customer_retention.new_customers_count IS 'кол-во новых клиентов';
COMMENT ON COLUMN mart.f_customer_retention.returning_customers_count IS 'кол-во вернувшихся клиентов';
COMMENT ON COLUMN mart.f_customer_retention.refunded_customer_count IS 'кол-во клиентов, оформивших возврат';
COMMENT ON COLUMN mart.f_customer_retention.period_name IS 'рассматирваемый период';
COMMENT ON COLUMN mart.f_customer_retention.period_id IS 'идентификатор периода (номер недели или номер месяца)';
COMMENT ON COLUMN mart.f_customer_retention.item_id IS 'идентификатор категории товара';
COMMENT ON COLUMN mart.f_customer_retention.new_customers_revenue IS 'доход с новых клиентов';
COMMENT ON COLUMN mart.f_customer_retention.returning_customers_revenue IS 'доход с вернувшихся клиентов';
COMMENT ON COLUMN mart.f_customer_retention.customers_refunded IS 'количество возвратов клиентов';


DROP PROCEDURE IF EXISTS mart.proc_customer_retention_insert;
CREATE PROCEDURE mart.proc_customer_retention_insert (i_period_name TEXT) 
LANGUAGE plpgsql AS 
$$
BEGIN
    
    IF NOT (i_period_name ILIKE 'MONTH' OR i_period_name ILIKE 'WEEK') THEN
        RAISE EXCEPTION 'Параметр period_name может принимает только значение "MONTH" или "WEEK"';
    END IF;
    
    
    DELETE
      FROM mart.f_customer_retention fcr
     WHERE fcr.period_name = CASE WHEN i_period_name ILIKE 'WEEK' THEN 'weekly'
                                  WHEN i_period_name ILIKE 'MONTH' THEN 'monthly'
                                   END;

    WITH date_encoding AS (
        SELECT dc.date_id
             , CASE WHEN i_period_name ILIKE 'WEEK' THEN dc.week_of_year
                    WHEN i_period_name ILIKE 'MONTH' THEN dc.month_actual
                END         AS period_id
             , CASE WHEN i_period_name ILIKE 'WEEK' THEN 'weekly'
                    WHEN i_period_name ILIKE 'MONTH' THEN 'monthly'
                END         AS period_name
          FROM mart.d_calendar dc 
    )
    
    , orders_per_daterange AS (
        SELECT fss.customer_id
             , fss.item_id
             , de.period_id
             , de.period_name
             , fss.status
             , COUNT(fss.id) AS orders_amount
             , SUM(fss.payment_amount) AS payment_amount
          FROM mart.f_sales fss
          JOIN date_encoding de
            ON de.date_id = fss.date_id   
         GROUP BY fss.customer_id
                , fss.item_id
                , de.period_id
                , de.period_name
                , fss.status
    )
    
    , insert_info AS (
    SELECT opd.period_id
         , opd.period_name
         , opd.item_id
         , COUNT(DISTINCT CASE WHEN opd.orders_amount = 1 THEN opd.customer_id ELSE NULL END)   AS new_customers_count 
         , COUNT(DISTINCT CASE WHEN opd.orders_amount > 1 THEN opd.customer_id ELSE NULL END)   AS returning_customers_count  
         , COUNT(DISTINCT CASE WHEN opd.status = 'refunded' THEN opd.customer_id ELSE NULL END) AS refunded_customer_count
         , SUM(CASE WHEN opd.orders_amount = 1 THEN opd.payment_amount ELSE 0 END)              AS new_customers_revenue
         , SUM(CASE WHEN opd.orders_amount > 1 THEN opd.payment_amount ELSE 0 END)              AS returning_customers_revenue
         , SUM(CASE WHEN opd.status = 'refunded' THEN opd.orders_amount ELSE 0 END)             AS customers_refunded
      FROM orders_per_daterange opd  
     GROUP BY opd.period_id 
            , opd.period_name
            , opd.item_id
    )
    
    INSERT INTO mart.f_customer_retention(
          id
        , new_customers_count
        , returning_customers_count
        , refunded_customer_count
        , period_name
        , period_id
        , item_id
        , new_customers_revenue
        , returning_customers_revenue
        , customers_refunded
    )
    SELECT nextval('mart.f_customer_retention_id_seq'::regclass)
         , ii.new_customers_count
         , ii.returning_customers_count
         , ii.refunded_customer_count
         , ii.period_name
         , ii.period_id
         , ii.item_id
         , ii.new_customers_revenue
         , ii.returning_customers_revenue
         , ii.customers_refunded
      FROM insert_info ii;
END;
$$;