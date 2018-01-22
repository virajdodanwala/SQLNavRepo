CREATE OR REPLACE PACKAGE mubasher_oms.pkg_dc_at_reports
IS
    TYPE refcursor IS REF CURSOR;

    PROCEDURE sp_orders_for_dealers (pview          OUT refcursor,
                                     pd1                DATE,
                                     pd2                DATE,
                                     puserid            NUMBER,
                                     pexchange          VARCHAR2,
                                     pinstitution       NUMBER DEFAULT 1,
                                     pdecimals          NUMBER DEFAULT 2);

    PROCEDURE sp_get_customer_details (
        pview                 OUT refcursor,
        pm01_customer_id   IN     m01_customer.m01_customer_id%TYPE DEFAULT NULL,
        pt03_account_id    IN     t03_cash_account.t03_account_id%TYPE DEFAULT NULL);

    PROCEDURE sp_order_log (
        pview             OUT refcursor,
        pt01_orderno   IN     t01_order_summary_intraday.t01_orderno%TYPE);

    PROCEDURE sp_cheuvrex_trade_confimation (
        pview                OUT refcursor,
        pexchangeaccnumber       VARCHAR2,
        pfrom_date               DATE,
        pto_date                 DATE,
        pdecimalplaces           NUMBER,
        pbuyrate                 NUMBER,
        psellrate                NUMBER,
        pcommission              NUMBER,
        pcommissionrate          NUMBER);

    PROCEDURE sp_get_ft_shares_exceed_25_vol (pview             OUT refcursor,
                                              prowcnt           OUT INT,
                                              psortcolumn           VARCHAR2,
                                              pn1                   INT,
                                              pn2                   INT,
                                              psearchcriteria       VARCHAR2,
                                              pdate                 DATE);

    PROCEDURE sp_get_cli_shr_exceed_10_vol (pview             OUT refcursor,
                                            prowcnt           OUT INT,
                                            psortcolumn           VARCHAR2,
                                            pn1                   INT,
                                            pn2                   INT,
                                            psearchcriteria       VARCHAR2,
                                            pdate                 DATE);

    PROCEDURE sp_get_cancel_orders_preopen (pview             OUT refcursor,
                                            prowcnt           OUT INT,
                                            psortcolumn           VARCHAR2,
                                            pn1                   INT,
                                            pn2                   INT,
                                            psearchcriteria       VARCHAR2,
                                            pdate                 DATE);

    PROCEDURE sp_get_ords_wit_bs_same_symbol (pview             OUT refcursor,
                                              prowcnt           OUT INT,
                                              psortcolumn           VARCHAR2,
                                              pn1                   INT,
                                              pn2                   INT,
                                              psearchcriteria       VARCHAR2,
                                              pdate                 DATE);

    PROCEDURE sp_get_can_ords_bs_same_symbol (pview             OUT refcursor,
                                              prowcnt           OUT INT,
                                              psortcolumn           VARCHAR2,
                                              pn1                   INT,
                                              pn2                   INT,
                                              psearchcriteria       VARCHAR2,
                                              pdate                 DATE);

    PROCEDURE sp_get_doubled_qty_orders (pview             OUT refcursor,
                                         prowcnt           OUT INT,
                                         psortcolumn           VARCHAR2,
                                         pn1                   INT,
                                         pn2                   INT,
                                         psearchcriteria       VARCHAR2,
                                         pdate                 DATE);

    PROCEDURE sp_get_lst_mnt_price_chg_ords (pview             OUT refcursor,
                                             prowcnt           OUT INT,
                                             psortcolumn           VARCHAR2,
                                             pn1                   INT,
                                             pn2                   INT,
                                             psearchcriteria       VARCHAR2,
                                             pdate                 DATE);

    PROCEDURE sp_get_adv_portfolio_summery (
        pview                 OUT refcursor,
        pt04_security_ac_id       NUMBER,
        pt04_exchange             VARCHAR2,
        pfrom_date                DATE,
        pto_date                  DATE);

    PROCEDURE sp_sg_fcg_internal_revenue (pview             OUT refcursor,
                                          prowcnt           OUT INT,
                                          psortcolumn           VARCHAR2,
                                          pn1                   INT,
                                          pn2                   INT,
                                          psearchcriteria       VARCHAR2,
                                          pd1                   DATE,
                                          pd2                   DATE,
                                          pcurrancy             VARCHAR2);

    PROCEDURE sp_exchange_settlement_rpt (pview OUT refcursor, pdate DATE);

    PROCEDURE sp_commission_report (pview       OUT refcursor,
                                    pfromdate       DATE,
                                    ptodate         DATE,
                                    pinst_id        NUMBER,
                                    pcurrency       VARCHAR2);

    PROCEDURE sp_commission_breakdown_report (pview       OUT refcursor,
                                              pfromdate       DATE,
                                              ptodate         DATE,
                                              pexchange       VARCHAR2,
                                              pcurrency       VARCHAR2);

    PROCEDURE sp_t12_pending_cash_summary (
        pview             OUT refcursor,
        prowcnt           OUT INT,
        psortcolumn           VARCHAR2,
        pn1                   INT,
        pn2                   INT,
        psearchcriteria       VARCHAR2,
        ptstatus              VARCHAR2 DEFAULT '0,1,2,4,6');
END;
/


CREATE OR REPLACE PACKAGE BODY mubasher_oms.pkg_dc_at_reports
IS
    PROCEDURE sp_orders_for_dealers (pview          OUT refcursor,
                                     pd1                DATE,
                                     pd2                DATE,
                                     puserid            NUMBER,
                                     pexchange          VARCHAR2,
                                     pinstitution       NUMBER DEFAULT 1,
                                     pdecimals          NUMBER DEFAULT 2)
    IS
    BEGIN
        IF TRUNC (pd1) < TRUNC (SYSDATE)
        THEN
            OPEN pview FOR
                  SELECT t01.t01_userid,
                         m04.m04_login_name,
                         UPPER (
                             TRIM (
                                    NVL (m06.m06_other_names, '')
                                 || ' '
                                 || NVL (m06.m06_last_name, '')))
                             AS dealername,
                         t01.t01_ordstatus,
                         m16.m16_descriotion,
                         m16.m16_descriotion_1,
                         t01.t01_createddate,
                         t01.t01_orderno,
                         t01.t01_orderid,
                         t01.t01_symbol,
                         t01.t01_side,
                         m13.m13_description_1,
                         t01.t01_ordertype,
                         m14.m14_description_1,
                         t01.t01_exchange,
                         CASE
                             WHEN t01.t01_instrument_type != 75
                             THEN
                                 t01.t01_orderqty
                         END
                             AS t01_orderqty,
                         CASE
                             WHEN t01.t01_cumqty > 0 THEN t01.t01_cumqty
                             ELSE 0
                         END
                             AS t01_cumqty,
                         t01.t01_currency,
                         ROUND (
                             (  t01.t01_price
                              * DECODE (t01.t01_price_ratio,
                                        0, 1,
                                        t01.t01_price_ratio)),
                             pdecimals)
                             t01_price,
                         ROUND (t01.t01_ordvalue, pdecimals) AS t01_ordvalue,
                         ROUND (t01.t01_commission, pdecimals)
                             AS t01_commission,
                         m01.m01_c1_customer_id,
                         m01.m01_external_ref_no,
                         t01.t01_inst_id,
                         m77.m77_short_description_en AS m107_short_description,
                         t01.t01_routingac,
                         TRIM (
                                TRIM (
                                       TRIM (
                                              NVL (m01.m01_c1_other_names, '')
                                           || ' '
                                           || NVL (m01.m01_c1_last_name, ''))
                                    || ' '
                                    || NVL (m01.m01_c1_second_name, ''))
                             || ' '
                             || NVL (m01.m01_c1_third_name, ''))
                             AS custname
                    FROM t01_order_summary_intraday_all t01,
                         (SELECT m16_status_id,
                                 m16_descriotion_1,
                                 CASE
                                     WHEN m16_status_id IN ('4', 'r')
                                     THEN
                                         'Cancelled'
                                     WHEN m16_status_id IN
                                              ('m',
                                               'f',
                                               'q',
                                               '1',
                                               '2',
                                               '0',
                                               'C')
                                     THEN
                                         'New Orders'
                                     ELSE
                                         TO_CHAR (m16_descriotion_1)
                                 END
                                     AS m16_descriotion
                            FROM m16_order_status) m16,
                         m14_order_type m14,
                         m13_order_side m13,
                         m01_customer m01,
                         m06_employees m06,
                         m04_logins m04,
                         m77_symbols m77
                   WHERE     t01.t01_ordstatus = m16.m16_status_id
                         AND t01.t01_ordertype = m14.m14_type_id
                         AND t01.t01_mubasher_no = m01.m01_c1_customer_id
                         AND t01.t01_userid = m06.m06_login_id
                         AND t01.t01_userid = m04.m04_id
                         AND t01.t01_exchange = m77.m77_exchange(+)
                         AND t01.t01_symbol = m77.m77_symbol(+)
                         AND t01.t01_side = m13.m13_side_id
                         AND t01_inst_id = pinstitution
                         AND TRUNC (t01.t01_createddate) >= TRUNC (pd1)
                         AND TRUNC (t01.t01_createddate) <= TRUNC (pd2)
                         AND t01.t01_userid = NVL (puserid, t01.t01_userid)
                         AND t01.t01_exchange =
                                 NVL (pexchange, t01.t01_exchange)
                ORDER BY dealername,
                         m16_descriotion,
                         m16_descriotion_1,
                         t01_orderno;
        ELSE
            OPEN pview FOR
                  SELECT t01.t01_userid,
                         m04.m04_login_name,
                         UPPER (
                             TRIM (
                                    NVL (m06.m06_other_names, '')
                                 || ' '
                                 || NVL (m06.m06_last_name, '')))
                             AS dealername,
                         t01.t01_ordstatus,
                         m16.m16_descriotion,
                         m16.m16_descriotion_1,
                         t01.t01_createddate,
                         t01.t01_orderno,
                         t01.t01_orderid,
                         t01.t01_symbol,
                         t01.t01_side,
                         m13.m13_description_1,
                         t01.t01_ordertype,
                         m14.m14_description_1,
                         t01.t01_exchange,
                         CASE
                             WHEN t01.t01_instrument_type != 75
                             THEN
                                 t01.t01_orderqty
                         END
                             AS t01_orderqty,
                         CASE
                             WHEN t01.t01_cumqty > 0 THEN t01.t01_cumqty
                             ELSE 0
                         END
                             AS t01_cumqty,
                         t01.t01_currency,
                         ROUND (
                             (  t01.t01_price
                              * DECODE (t01.t01_price_ratio,
                                        0, 1,
                                        t01.t01_price_ratio)),
                             pdecimals)
                             t01_price,
                         ROUND (t01.t01_ordvalue, pdecimals) AS t01_ordvalue,
                         ROUND (t01.t01_commission, pdecimals)
                             AS t01_commission,
                         m01.m01_c1_customer_id,
                         m01.m01_external_ref_no,
                         t01.t01_inst_id,
                         m77.m77_short_description_en AS m107_short_description,
                         t01.t01_routingac
                    FROM t01_order_summary_intraday t01,
                         (SELECT m16_status_id,
                                 m16_descriotion_1,
                                 CASE
                                     WHEN m16_status_id IN ('4', 'r')
                                     THEN
                                         'Cancelled'
                                     WHEN m16_status_id IN
                                              ('m',
                                               'f',
                                               'q',
                                               '1',
                                               '2',
                                               '0',
                                               'C')
                                     THEN
                                         'New Orders'
                                     ELSE
                                         TO_CHAR (m16_descriotion_1)
                                 END
                                     AS m16_descriotion
                            FROM m16_order_status) m16,
                         m14_order_type m14,
                         m13_order_side m13,
                         m01_customer m01,
                         m06_employees m06,
                         m04_logins m04,
                         m77_symbols m77
                   WHERE     t01.t01_ordstatus = m16.m16_status_id
                         AND t01.t01_ordertype = m14.m14_type_id
                         AND t01.t01_mubasher_no = m01.m01_c1_customer_id
                         AND t01.t01_userid = m06.m06_login_id
                         AND t01.t01_userid = m04.m04_id
                         AND t01.t01_exchange = m77.m77_exchange(+)
                         AND t01.t01_symbol = m77.m77_symbol(+)
                         AND t01.t01_side = m13.m13_side_id
                         AND t01_inst_id = pinstitution
                         AND TRUNC (t01.t01_createddate) >= TRUNC (pd1)
                         AND TRUNC (t01.t01_createddate) <= TRUNC (pd2)
                         AND t01.t01_userid = NVL (puserid, t01.t01_userid)
                         AND t01.t01_exchange =
                                 NVL (pexchange, t01.t01_exchange)
                ORDER BY dealername,
                         m16_descriotion,
                         m16_descriotion_1,
                         t01_orderno;
        END IF;
    END;

    PROCEDURE sp_get_customer_details (
        pview                 OUT refcursor,
        pm01_customer_id   IN     m01_customer.m01_customer_id%TYPE DEFAULT NULL,
        pt03_account_id    IN     t03_cash_account.t03_account_id%TYPE DEFAULT NULL)
    IS
        l_m01_customer_id       m01_customer.m01_customer_id%TYPE;
        l_t03_investor_acc_no   t03_cash_account.t03_investor_acc_no%TYPE;
        l_t03_currency          t03_cash_account.t03_currency%TYPE;
        l_t03_iban_no           t03_cash_account.t03_iban_no%TYPE;
    BEGIN
        IF pt03_account_id IS NOT NULL
        THEN
            SELECT t03_investor_acc_no,
                   t03_currency,
                   t03_iban_no,
                   t03_profile_id
              INTO l_t03_investor_acc_no,
                   l_t03_currency,
                   l_t03_iban_no,
                   l_m01_customer_id
              FROM t03_cash_account
             WHERE t03_account_id = pt03_account_id;
        ELSE
            l_m01_customer_id := pm01_customer_id;
        END IF;

        OPEN pview FOR
            SELECT m01.m01_customer_id,
                   m01.m01_c1_customer_id,
                   m01.m01_external_ref_no,
                   m01.m01_prefered_language,
                   TRIM (m01.m01_c1_arabic_name) AS m01_c1_arabic_name,
                   TRIM (m01_c1_arabic_first_name)
                       AS m01_c1_arabic_first_name,
                   TRIM (m01_c1_arabic_second_name)
                       AS m01_c1_arabic_second_name,
                   TRIM (m01.m01_c1_arabic_third_name)
                       AS m01_c1_arabic_third_name,
                   TRIM (
                          TRIM (
                                 TRIM (
                                        NVL (m01.m01_c1_other_names, '')
                                     || ' '
                                     || NVL (m01.m01_c1_last_name, ''))
                              || ' '
                              || NVL (m01.m01_c1_second_name, ''))
                       || ' '
                       || NVL (m01.m01_c1_third_name, ''))
                       AS custname,
                   TRIM (
                          TRIM (
                                 TRIM (
                                        NVL (m01.m01_c1_arabic_first_name,
                                             '')
                                     || ' '
                                     || NVL (m01.m01_c1_arabic_name, ''))
                              || ' '
                              || NVL (m01.m01_c1_arabic_second_name, ''))
                       || ' '
                       || NVL (m01.m01_c1_arabic_third_name, ''))
                       AS custname_ar,
                   TRIM (m01.m01_c1_pobox) AS m01_c1_pobox,
                   TRIM (m01.m01_c1_zip) AS m01_c1_zip,
                   TRIM (m01.m01_extra_no) AS m01_extra_no,
                   TRIM (m01.m01_c1_street_address_1)
                       AS m01_c1_street_address_1,
                   TRIM (m01.m01_c1_street_address_2)
                       AS m01_c1_street_address_2,
                   TRIM (m01.m01_c1_city) AS m01_c1_city,
                   TRIM (m30.m30_country_name) AS m30_country_name,
                   TRIM (m30.m30_arabic_name) AS m30_arabic_name,
                   TRIM (m01.m01_c1_arabic_address) AS m01_c1_arabic_address,
                   TRIM (m130.m130_description) AS m130_description,
                   TRIM (m130.m130_arabic_description)
                       AS m130_arabic_description,
                   TRIM (m158.m158_description) AS m158_description,
                   TRIM (m158.m158_description_ar) AS m158_description_ar,
                   l_t03_investor_acc_no AS t03_investor_acc_no,
                   l_t03_currency AS t03_currency,
                   l_t03_iban_no AS t03_iban_no,
                   m01.m01_mail_preference,
                   m01.m01_c1_email
              FROM m01_customer m01,
                   m30_country m30,
                   m130_titles m130,
                   m158_cities m158
             WHERE     m01.m01_country_id = m30.m30_country_id(+)
                   AND m01.m01_title = m130.m130_id(+)
                   AND m01.m01_city = m158.m158_id(+)
                   AND m01.m01_customer_id = l_m01_customer_id;
    END;

    PROCEDURE sp_order_log (
        pview             OUT refcursor,
        pt01_orderno   IN     t01_order_summary_intraday.t01_orderno%TYPE)
    IS
    BEGIN
        OPEN pview FOR
            SELECT t01_clordid,
                   CASE
                       WHEN ROWNUM = 1
                       THEN
                           'New'
                       WHEN t01_ordstatus IN ('h', '8', '1', '2', 'C', '4')
                       THEN
                           TO_CHAR (m16_descriotion_1)
                       ELSE
                           'Modify'
                   END
                       AS event,
                   t01_createddate,
                   m04_login_name,
                   UPPER (
                       TRIM (
                              NVL (m06_other_names, '')
                           || ' '
                           || NVL (m06_last_name, '')))
                       AS dealername,
                   CASE
                       WHEN ROWNUM = 1
                       THEN
                              (CASE t01_side
                                   WHEN 1 THEN 'Buy '
                                   WHEN 2 THEN 'Sell '
                               END)
                           || t01_orderqty
                           || ' @ '
                           || t01_price
                       ELSE
                           NULL
                   END
                       AS description,
                   t01_price,
                   t01_orderqty,
                   t01_leavesqty,
                   t01_expiretime,
                   t01_minqty
              FROM (  SELECT t01.t01_clordid,
                             t01.t01_ordstatus,
                             m16.m16_descriotion_1,
                             t01.t01_createddate,
                             m04.m04_login_name,
                             m06.m06_other_names,
                             m06.m06_last_name,
                             t01.t01_side,
                             t01.t01_orderqty,
                             t01.t01_price,
                             t01.t01_leavesqty,
                             t01_expiretime,
                             t01_minqty
                        FROM t01_order_summary_intraday t01,
                             m16_order_status m16,
                             m06_employees m06,
                             m04_logins m04
                       WHERE     t01.t01_ordstatus = m16.m16_status_id
                             AND t01.t01_userid = m06.m06_login_id(+)
                             AND t01.t01_userid = m04.m04_id(+)
                             AND t01.t01_orderno = pt01_orderno
                    ORDER BY t01_clordid);
    END;

    PROCEDURE sp_cheuvrex_trade_confimation (
        pview                OUT refcursor,
        pexchangeaccnumber       VARCHAR2,
        pfrom_date               DATE,
        pto_date                 DATE,
        pdecimalplaces           NUMBER,
        pbuyrate                 NUMBER,
        psellrate                NUMBER,
        pcommission              NUMBER,
        pcommissionrate          NUMBER)
    IS
    BEGIN
        OPEN pview FOR
              SELECT a.t05_code,
                     CASE WHEN a.t05_code = 'STLBUY' THEN 'Buy' ELSE 'Sell' END
                         AS side,
                     CASE
                         WHEN a.t05_code = 'STLBUY' THEN pbuyrate
                         ELSE psellrate
                     END
                         AS fx_rate,
                     a.t05_orderno,
                     TRUNC (a.t05_date) t05_date,
                     a.t05_symbol,
                     TRUNC (a.t05_value_date) t05_value_date,
                     a.t05_exchange,
                     b.m77_reuters_code,
                     b.m77_isincode,
                     b.m77_short_description_en AS m107_short_description,
                     SUM (a.t05_last_shares) t05_last_shares,
                     ROUND (AVG (a.t05_lastpx), 8) t05_lastpx,
                     SUM (a.t05_amount) t05_amount,
                     SUM (a.t05_commission) t05_commission,
                     CASE
                         WHEN a.t05_code = 'STLBUY'
                         THEN
                             CASE
                                 WHEN pcommission = 1
                                 THEN
                                     ROUND (
                                         ( (  ABS (SUM (a.t05_amount))
                                            + ABS (SUM (a.t05_commission)))),
                                         pdecimalplaces)
                                 WHEN pcommission IN (2, 3)
                                 THEN
                                     ROUND (
                                         (ABS (
                                                SUM (a.t05_amount)
                                              + (  SUM (a.t05_amount)
                                                 * (pcommissionrate / 100)))),
                                         pdecimalplaces)
                             END
                         WHEN a.t05_code = 'STLSEL'
                         THEN
                             CASE
                                 WHEN pcommission = 1
                                 THEN
                                     ROUND (
                                         ( (  ABS (SUM (a.t05_amount))
                                            - ABS (SUM (a.t05_commission)))),
                                         pdecimalplaces)
                                 WHEN pcommission IN (2, 3)
                                 THEN
                                     ROUND (
                                         (ABS (
                                                SUM (a.t05_amount)
                                              - (  SUM (a.t05_amount)
                                                 * (pcommissionrate / 100)))),
                                         pdecimalplaces)
                             END
                     END
                         net_amount,
                     0 AS other_charges
                FROM t05_cash_account_log a, m77_symbols b
               WHERE     a.t05_cash_account_id IN
                             (SELECT q.u05_cash_account_id
                                FROM u06_routing_accounts p,
                                     u05_security_accounts q
                               WHERE     p.u06_exchange IN
                                             (SELECT m11_exchangecode
                                                FROM m11_exchanges
                                               WHERE m11_is_local = 1)
                                     AND p.u06_exchange_ac = pexchangeaccnumber
                                     AND p.u06_security_ac_id = q.u05_id)
                     AND a.t05_date BETWEEN pfrom_date AND pto_date + 0.99999
                     AND a.t05_symbol = b.m77_symbol
                     AND a.t05_exchange = b.m77_exchange
                     AND a.t05_code IN ('STLBUY', 'STLSEL')
            GROUP BY a.t05_code,
                     a.t05_orderno,
                     TRUNC (a.t05_date),
                     a.t05_symbol,
                     TRUNC (a.t05_value_date),
                     a.t05_exchange,
                     b.m77_reuters_code,
                     b.m77_isincode,
                     b.m77_short_description_en;
    END;

    PROCEDURE sp_get_ft_shares_exceed_25_vol (pview             OUT refcursor,
                                              prowcnt           OUT INT,
                                              psortcolumn           VARCHAR2,
                                              pn1                   INT,
                                              pn2                   INT,
                                              psearchcriteria       VARCHAR2,
                                              pdate                 DATE)
    IS
    BEGIN
        OPEN pview FOR
            SELECT ord_table.symbol AS symbol,
                   ord_table.tdwl_volume AS tdwl_volume,
                   ord_table.closing_price AS closing_price,
                   ord_table.ft_volume AS ft_volume,
                   ord_table.ft_percentage AS ft_percentage,
                   ord_table.client_volume AS client_volume,
                   ord_table.client_percentage AS client_percentage,
                   ord_table.t01_createddate AS order_date,
                   pkg_dc_m243_customer_id.m243_get_customer_id_no (
                       m01.m01_customer_id)
                       AS customer_id,
                   TRIM (
                          TRIM (
                                 TRIM (
                                        NVL (m01.m01_c1_other_names, '')
                                     || ' '
                                     || NVL (m01.m01_c1_last_name, ''))
                              || ' '
                              || NVL (m01.m01_c1_second_name, ''))
                       || ' '
                       || NVL (m01.m01_c1_third_name, ''))
                       AS customer_name
              FROM (SELECT ft_tbl.symbol AS symbol,
                           ft_tbl.volume AS tdwl_volume,
                           ROUND (ft_tbl.close, 2) AS closing_price,
                           ft_tbl.t01_cumqty AS ft_volume,
                           ft_tbl.ft_percentage AS ft_percentage,
                           cl_tbl.t01_cumqty AS client_volume,
                           cl_tbl.t01_createddate,
                           cl_tbl.t01_m01_customer_id AS customer_id,
                           ROUND (cl_tbl.t01_cumqty / ft_tbl.volume * 100, 2)
                               AS client_percentage
                      FROM (SELECT a04.symbol,
                                   a04.volume,
                                   a04.close,
                                   t01.t01_cumqty,
                                   ROUND (
                                       (t01.t01_cumqty / a04.volume) * 100,
                                       2)
                                       AS ft_percentage
                              FROM (SELECT a.symbol, a.volume, a.close
                                      FROM mubasher_price.esp_transactions_complete a
                                     WHERE     a.transactiondate BETWEEN TO_DATE (
                                                                             pdate)
                                                                     AND   TO_DATE (
                                                                               pdate)
                                                                         + .99999
                                           AND a.exchangecode IN
                                                   (SELECT m11_exchangecode
                                                      FROM m11_exchanges
                                                     WHERE m11_is_local = 1)
                                    UNION
                                    SELECT a.symbol AS symbol,
                                           a.volume AS volume,
                                           CASE
                                               WHEN a.todaysclosed = 0
                                               THEN
                                                   a.previousclosed
                                               ELSE
                                                   a.todaysclosed
                                           END
                                               AS close
                                      FROM mubasher_price.esp_todays_snapshots a
                                     WHERE a.exchangecode IN
                                               (SELECT m11_exchangecode
                                                  FROM m11_exchanges
                                                 WHERE m11_is_local = 1)) a04,
                                   (  SELECT SUM (b.t01_cumqty) AS t01_cumqty,
                                             b.t01_symbol
                                        FROM t01_order_summary_intraday_all b
                                       WHERE     b.t01_createddate BETWEEN TO_DATE (
                                                                               pdate)
                                                                       AND   TO_DATE (
                                                                                 pdate)
                                                                           + .99999
                                             AND b.t01_exchange IN
                                                     (SELECT m11_exchangecode
                                                        FROM m11_exchanges
                                                       WHERE m11_is_local = 1)
                                             AND b.t01_ordstatus IN
                                                     ('1', '2', 'q', 'r')
                                    GROUP BY b.t01_symbol) t01
                             WHERE     a04.symbol = t01.t01_symbol
                                   AND a04.volume > 0
                                   AND t01.t01_cumqty / a04.volume >= 0.25) ft_tbl,
                           (  SELECT SUM (c.t01_cumqty) AS t01_cumqty,
                                     c.t01_symbol,
                                     MAX (c.t01_createddate) AS t01_createddate,
                                     c.t01_m01_customer_id
                                FROM t01_order_summary_intraday_all c
                               WHERE     c.t01_createddate BETWEEN TO_DATE (
                                                                       pdate)
                                                               AND   TO_DATE (
                                                                         pdate)
                                                                   + .99999
                                     AND c.t01_exchange IN
                                             (SELECT m11_exchangecode
                                                FROM m11_exchanges
                                               WHERE m11_is_local = 1)
                                     AND c.t01_ordstatus IN
                                             ('1', '2', 'q', 'r')
                            GROUP BY c.t01_m01_customer_id, c.t01_symbol) cl_tbl
                     WHERE cl_tbl.t01_symbol = ft_tbl.symbol) ord_table,
                   m01_customer m01
             WHERE ord_table.customer_id = m01.m01_customer_id;
    END;

    PROCEDURE sp_get_cli_shr_exceed_10_vol (pview             OUT refcursor,
                                            prowcnt           OUT INT,
                                            psortcolumn           VARCHAR2,
                                            pn1                   INT,
                                            pn2                   INT,
                                            psearchcriteria       VARCHAR2,
                                            pdate                 DATE)
    IS
    BEGIN
        OPEN pview FOR
            SELECT abc.CURRENT_DATE,
                   abc.customer_id,
                   abc.customer_name,
                   stock_table.symbol AS stock_code,
                   abc.client_volume,
                   stock_table.volume AS tdwl_volume,
                   stock_table.close AS closing_price,
                   ROUND ( ( (abc.client_volume / stock_table.volume) * 100),
                          2)
                       AS client_percentage
              FROM (  SELECT TRUNC (MAX (t01.t01_createddate)) AS CURRENT_DATE,
                             MAX (
                                 pkg_dc_m243_customer_id.m243_get_customer_id_no (
                                     m01.m01_customer_id))
                                 AS customer_id,
                             MAX (
                                 TRIM (
                                        TRIM (
                                               TRIM (
                                                      NVL (
                                                          m01.m01_c1_other_names,
                                                          '')
                                                   || ' '
                                                   || NVL (
                                                          m01.m01_c1_last_name,
                                                          ''))
                                            || ' '
                                            || NVL (m01.m01_c1_second_name, ''))
                                     || ' '
                                     || NVL (m01.m01_c1_third_name, '')))
                                 AS customer_name,
                             SUM (t01.t01_cumqty) AS client_volume,
                             t01.t01_symbol,
                             t01.t01_m01_customer_id AS cus_id
                        FROM t01_order_summary_intraday_all t01,
                             m01_customer m01
                       WHERE     t01.t01_m01_customer_id = m01.m01_customer_id
                             AND t01.t01_ordstatus IN ('1', '2')
                             AND t01.t01_exchange IN (SELECT m11_exchangecode
                                                        FROM m11_exchanges
                                                       WHERE m11_is_local = 1)
                             AND t01.t01_createddate BETWEEN TO_DATE (pdate)
                                                         AND   TO_DATE (pdate)
                                                             + 0.99999
                    GROUP BY t01.t01_symbol, t01.t01_m01_customer_id) abc,
                   (SELECT a.symbol, a.volume, a.close
                      FROM mubasher_price.esp_transactions_complete a
                     WHERE     a.transactiondate BETWEEN TO_DATE (pdate)
                                                     AND   TO_DATE (pdate)
                                                         + .99999
                           AND a.exchangecode IN (SELECT m11_exchangecode
                                                    FROM m11_exchanges
                                                   WHERE m11_is_local = 1)
                    UNION
                    SELECT a.symbol AS symbol,
                           a.volume AS volume,
                           CASE
                               WHEN a.todaysclosed = 0 THEN a.previousclosed
                               ELSE a.todaysclosed
                           END
                               AS close
                      FROM mubasher_price.esp_todays_snapshots a
                     WHERE a.exchangecode IN (SELECT m11_exchangecode
                                                FROM m11_exchanges
                                               WHERE m11_is_local = 1)) stock_table
             WHERE     abc.t01_symbol = stock_table.symbol
                   AND stock_table.volume > 0
                   AND (stock_table.volume / 10) <= abc.client_volume;
    END;

    PROCEDURE sp_get_cancel_orders_preopen (pview             OUT refcursor,
                                            prowcnt           OUT INT,
                                            psortcolumn           VARCHAR2,
                                            pn1                   INT,
                                            pn2                   INT,
                                            psearchcriteria       VARCHAR2,
                                            pdate                 DATE)
    IS
    BEGIN
        OPEN pview FOR
            SELECT t01.customer_name,
                   pkg_dc_m243_customer_id.m243_get_customer_id_no (
                       t01.m01_customer_id)
                       AS customer_id,
                   t01.t01_symbol AS stock_code,
                   t01.ord_type AS order_type,
                   t01.t01_last_updated AS cancellation_time,
                   t01.t01_orderqty AS order_qty,
                   t01.t01_price AS order_price,
                   a04.close AS closing_price,
                   ROUND ( ( (t01.t01_price - a04.close) * 100 / a04.close),
                          2)
                       AS changed_percentage
              FROM (SELECT t01.t01_symbol,
                           t01.t01_price,
                           t01.t01_orderqty,
                           CASE
                               WHEN t01.t01_side = 1 THEN 'Buy'
                               WHEN t01.t01_side = 2 THEN 'Sell'
                           END
                               AS ord_type,
                           t01.t01_last_updated,
                           t01.t01_createddate,
                           TRIM (
                                  TRIM (
                                         TRIM (
                                                NVL (m01.m01_c1_other_names,
                                                     '')
                                             || ' '
                                             || NVL (m01.m01_c1_last_name,
                                                     ''))
                                      || ' '
                                      || NVL (m01.m01_c1_second_name, ''))
                               || ' '
                               || NVL (m01.m01_c1_third_name, ''))
                               AS customer_name,
                           m01.m01_customer_id
                      FROM t01_order_summary_intraday_all t01,
                           m01_customer m01
                     WHERE     t01.t01_m01_customer_id = m01.m01_customer_id
                           AND t01.t01_createddate BETWEEN TO_DATE (pdate)
                                                       AND   TO_DATE (pdate)
                                                           + 0.999999
                           AND t01.t01_exchange IN (SELECT m11_exchangecode
                                                      FROM m11_exchanges
                                                     WHERE m11_is_local = 1)
                           AND TO_NUMBER (TO_CHAR (t01_last_updated, 'hh24')) >=
                                   10
                           AND TO_NUMBER (TO_CHAR (t01_last_updated, 'hh24')) <
                                   11
                           AND t01_ordstatus = '4') t01,
                   (SELECT a.close, a.symbol
                      FROM mubasher_price.esp_transactions_complete a
                     WHERE     a.transactiondate BETWEEN TO_DATE (pdate)
                                                     AND   TO_DATE (pdate)
                                                         + .99999
                           AND a.exchangecode IN (SELECT m11_exchangecode
                                                    FROM m11_exchanges
                                                   WHERE m11_is_local = 1)
                    UNION
                    SELECT CASE
                               WHEN a.todaysclosed = 0 THEN a.previousclosed
                               ELSE a.todaysclosed
                           END
                               AS close,
                           a.symbol AS symbol
                      FROM mubasher_price.esp_todays_snapshots a
                     WHERE a.exchangecode IN (SELECT m11_exchangecode
                                                FROM m11_exchanges
                                               WHERE m11_is_local = 1)) a04
             WHERE t01.t01_symbol = a04.symbol;
    END;

    PROCEDURE sp_get_ords_wit_bs_same_symbol (pview             OUT refcursor,
                                              prowcnt           OUT INT,
                                              psortcolumn           VARCHAR2,
                                              pn1                   INT,
                                              pn2                   INT,
                                              psearchcriteria       VARCHAR2,
                                              pdate                 DATE)
    IS
    BEGIN
        OPEN pview FOR
            SELECT ord_table.t01_createddate AS order_date,
                   (pkg_dc_m243_customer_id.m243_get_customer_id_no (
                        m01.m01_customer_id))
                       AS id_number,
                   (TRIM (
                           TRIM (
                                  TRIM (
                                         NVL (m01.m01_c1_other_names, '')
                                      || ' '
                                      || NVL (m01.m01_c1_last_name, ''))
                               || ' '
                               || NVL (m01.m01_c1_second_name, ''))
                        || ' '
                        || NVL (m01.m01_c1_third_name, '')))
                       AS customer_name,
                   ord_table.t01_symbol AS stock_code,
                   CASE
                       WHEN ord_table.t01_side = 1 THEN 'Buy'
                       WHEN ord_table.t01_side = 2 THEN 'Sell'
                   END
                       AS order_type,
                   ord_table.latest_ord_qty AS max_order_qty,
                   ord_table.initial_order_qty AS order_quantity,
                   ord_table.latest_ord_qty - ord_table.initial_order_qty
                       AS increased_by
              FROM m01_customer m01,
                   (SELECT t01_original.t01_clordid AS initial_ord_id,
                           t01_latest.t01_clordid AS latest_ord_id,
                           t01_original.t01_orderqty AS initial_order_qty,
                           t01_latest.t01_orderqty AS latest_ord_qty,
                           t01_original.t01_symbol,
                           t01_original.t01_createddate,
                           t01_original.t01_side,
                           t01_original.t01_m01_customer_id
                      FROM t01_order_summary_intraday_all t01_original,
                           (SELECT a.t01_orderno,
                                   a.t01_clordid,
                                   a.t01_orderqty,
                                   a.t01_symbol
                              FROM t01_order_summary_intraday_all a,
                                   (SELECT a.t01_m01_customer_id,
                                           a.t01_symbol
                                      FROM t01_order_summary_intraday a
                                     WHERE     a.t01_side = 1
                                           AND a.t01_createddate BETWEEN TO_DATE (
                                                                             pdate)
                                                                     AND   TO_DATE (
                                                                               pdate)
                                                                         + .99999
                                           AND a.t01_exchange IN
                                                   (SELECT m11_exchangecode
                                                      FROM m11_exchanges
                                                     WHERE m11_is_local = 1)
                                           AND a.t01_orderqty >= 10000
                                    INTERSECT
                                    SELECT a.t01_m01_customer_id,
                                           a.t01_symbol
                                      FROM t01_order_summary_intraday a
                                     WHERE     a.t01_side = 2
                                           AND a.t01_createddate BETWEEN TO_DATE (
                                                                             pdate)
                                                                     AND   TO_DATE (
                                                                               pdate)
                                                                         + .99999
                                           AND a.t01_exchange IN
                                                   (SELECT m11_exchangecode
                                                      FROM m11_exchanges
                                                     WHERE m11_is_local = 1)
                                           AND a.t01_orderqty >= 10000) cust
                             WHERE     a.t01_exchange IN
                                           (SELECT m11_exchangecode
                                              FROM m11_exchanges
                                             WHERE m11_is_local = 1)
                                   AND a.t01_ordstatus IN
                                           ('1',
                                            '2',
                                            'O',
                                            '5',
                                            '9',
                                            'C',
                                            'q',
                                            'r',
                                            'M',
                                            '0',
                                            '4')
                                   AND a.t01_origclordid <> -1
                                   AND a.t01_origclordid IS NOT NULL
                                   AND a.t01_orderqty >= 10000
                                   AND a.t01_createddate BETWEEN TO_DATE (
                                                                     pdate)
                                                             AND   TO_DATE (
                                                                       pdate)
                                                                 + .99999
                                   AND a.t01_symbol = cust.t01_symbol
                                   AND a.t01_m01_customer_id =
                                           cust.t01_m01_customer_id) t01_latest
                     WHERE     t01_original.t01_clordid =
                                   t01_latest.t01_orderno
                           AND t01_original.t01_orderqty <
                                   t01_latest.t01_orderqty) ord_table
             WHERE ord_table.t01_m01_customer_id = m01.m01_customer_id;
    END;

    PROCEDURE sp_get_can_ords_bs_same_symbol (pview             OUT refcursor,
                                              prowcnt           OUT INT,
                                              psortcolumn           VARCHAR2,
                                              pn1                   INT,
                                              pn2                   INT,
                                              psearchcriteria       VARCHAR2,
                                              pdate                 DATE)
    IS
    BEGIN
        OPEN pview FOR
            SELECT ord_table.t01_last_updated AS cancellation_time,
                   (pkg_dc_m243_customer_id.m243_get_customer_id_no (
                        m01.m01_customer_id))
                       AS id_number,
                   (TRIM (
                           TRIM (
                                  TRIM (
                                         NVL (m01.m01_c1_other_names, '')
                                      || ' '
                                      || NVL (m01.m01_c1_last_name, ''))
                               || ' '
                               || NVL (m01.m01_c1_second_name, ''))
                        || ' '
                        || NVL (m01.m01_c1_third_name, '')))
                       AS customer_name,
                   ord_table.t01_orderqty AS order_quantity,
                   ord_table.t01_symbol AS stock_code,
                   CASE
                       WHEN ord_table.t01_side = 1 THEN 'Buy'
                       WHEN ord_table.t01_side = 2 THEN 'Sell'
                   END
                       AS order_type
              FROM (  SELECT t01.t01_m01_customer_id,
                             t01.t01_symbol,
                             t01.t01_side,
                             MAX (t01.t01_last_updated) AS t01_last_updated,
                             SUM (t01.t01_orderqty) AS t01_orderqty
                        FROM t01_order_summary_intraday_all t01,
                             (SELECT DISTINCT t01s.*
                                FROM (  SELECT b.t01_symbol,
                                               b.t01_m01_customer_id,
                                               SUM (b.t01_orderqty) AS total_sell
                                          FROM t01_order_summary_intraday_all b
                                         WHERE     b.t01_side = 2
                                               AND b.t01_createddate BETWEEN TO_DATE (
                                                                                 pdate)
                                                                         AND   TO_DATE (
                                                                                   pdate)
                                                                             + 0.99999
                                               AND b.t01_exchange IN
                                                       (SELECT m11_exchangecode
                                                          FROM m11_exchanges
                                                         WHERE m11_is_local = 1)
                                               AND b.t01_ordstatus IN ('4')
                                      GROUP BY b.t01_m01_customer_id,
                                               b.t01_symbol) t01s,
                                     (  SELECT c.t01_symbol,
                                               c.t01_m01_customer_id,
                                               SUM (c.t01_orderqty) AS total_buy
                                          FROM t01_order_summary_intraday_all c
                                         WHERE     c.t01_side = 1
                                               AND c.t01_createddate BETWEEN TO_DATE (
                                                                                 pdate)
                                                                         AND   TO_DATE (
                                                                                   pdate)
                                                                             + 0.99999
                                               AND c.t01_exchange IN
                                                       (SELECT m11_exchangecode
                                                          FROM m11_exchanges
                                                         WHERE m11_is_local = 1)
                                               AND c.t01_ordstatus IN ('4')
                                      GROUP BY c.t01_m01_customer_id,
                                               c.t01_symbol) t01b
                               WHERE     t01s.t01_symbol = t01s.t01_symbol
                                     AND t01s.t01_m01_customer_id =
                                             t01b.t01_m01_customer_id
                                     AND t01s.total_sell >= 10000
                                     AND t01b.total_buy >= 10000) t01_g
                       WHERE     t01.t01_symbol = t01_g.t01_symbol
                             AND t01.t01_m01_customer_id =
                                     t01_g.t01_m01_customer_id
                             AND t01.t01_createddate BETWEEN TO_DATE (pdate)
                                                         AND   TO_DATE (pdate)
                                                             + 0.99999
                             AND t01.t01_exchange IN (SELECT m11_exchangecode
                                                        FROM m11_exchanges
                                                       WHERE m11_is_local = 1)
                             AND t01.t01_ordstatus IN ('4')
                    GROUP BY t01.t01_m01_customer_id,
                             t01.t01_symbol,
                             t01.t01_side) ord_table,
                   m01_customer m01
             WHERE     ord_table.t01_m01_customer_id = m01.m01_customer_id
                   AND TRUNC (ord_table.t01_last_updated) = TRUNC (pdate);
    END;

    PROCEDURE sp_get_doubled_qty_orders (pview             OUT refcursor,
                                         prowcnt           OUT INT,
                                         psortcolumn           VARCHAR2,
                                         pn1                   INT,
                                         pn2                   INT,
                                         psearchcriteria       VARCHAR2,
                                         pdate                 DATE)
    IS
    BEGIN
        /*
        table a : get t01.orders for min clordid where order_qty >= 10000 and created date between fromtime to totime for TDWL

        table b : get t01.orders for max clordid where order_qty >= 10000 and created date between fromtime to totime for TDWL

        select only where b.order_qty > a.order_qty

        2012/09/21 : Saranga
        */
        OPEN pview FOR
            SELECT DISTINCT
                   order_date,
                   stock_code,
                   min_order_qty AS qty_before,
                   max_order_qty AS qty_after,
                   amendments,
                   ord_type,
                   customer_id,
                   TRIM (
                          TRIM (
                                 TRIM (
                                        NVL (m01.m01_c1_other_names, '')
                                     || ' '
                                     || NVL (m01.m01_c1_last_name, ''))
                              || ' '
                              || NVL (m01.m01_c1_second_name, ''))
                       || ' '
                       || NVL (m01.m01_c1_third_name, ''))
                       AS customer_name,
                   pkg_dc_m243_customer_id.m243_get_customer_id_no (
                       m01.m01_customer_id)
                       AS id_number
              FROM (SELECT TRUNC (t01_createddate) AS order_date,
                           t01_symbol AS stock_code,
                           order_count - 1 AS amendments,
                           CASE
                               WHEN t01_side = 1 THEN 'Buy'
                               WHEN t01_side = 2 THEN 'Sell'
                           END
                               AS ord_type,
                           t01_orderno,
                           t01_orderqty,
                           t01_m01_customer_id AS customer_id,
                           MIN (t01_orderqty) OVER (PARTITION BY t01_orderno)
                               AS min_order_qty,
                           MAX (t01_orderqty) OVER (PARTITION BY t01_orderno)
                               AS max_order_qty
                      FROM (SELECT t01.t01_createddate,
                                   t01.t01_symbol,
                                   t01_side,
                                   t01.t01_clordid,
                                   t01.t01_orderqty,
                                   t01.t01_orderno,
                                   t01_m01_customer_id,
                                   MIN (t01.t01_clordid)
                                       OVER (PARTITION BY t01.t01_orderno)
                                       AS min_clordid,
                                   MAX (t01.t01_clordid)
                                       OVER (PARTITION BY t01.t01_orderno)
                                       AS max_clordid,
                                   COUNT (t01.t01_orderno)
                                       OVER (PARTITION BY t01.t01_orderno)
                                       AS order_count
                              FROM t01_order_summary_intraday_all t01
                             WHERE     t01.t01_exchange IN
                                           (SELECT m11_exchangecode
                                              FROM m11_exchanges
                                             WHERE m11_is_local = 1)
                                   AND t01.t01_createddate BETWEEN TRUNC (
                                                                       pdate)
                                                               AND   TRUNC (
                                                                         pdate)
                                                                   + 0.99999
                                   AND t01.t01_orderqty >= 10000) t01_all
                     WHERE     (   t01_all.t01_clordid = t01_all.min_clordid
                                OR t01_all.t01_clordid = t01_all.max_clordid)
                           AND order_count > 2) tt,
                   m01_customer m01
             WHERE     customer_id = m01.m01_customer_id
                   AND max_order_qty > min_order_qty;
    END;

    PROCEDURE sp_get_lst_mnt_price_chg_ords (pview             OUT refcursor,
                                             prowcnt           OUT INT,
                                             psortcolumn           VARCHAR2,
                                             pn1                   INT,
                                             pn2                   INT,
                                             psearchcriteria       VARCHAR2,
                                             pdate                 DATE)
    IS
    BEGIN
        OPEN pview FOR
            SELECT pkg_dc_m243_customer_id.m243_get_customer_id_no (
                       m01.m01_customer_id)
                       AS customer_id,
                   ord_details.t01_last_updated AS order_date,
                   ord_details.t01_orderid AS order_no,
                   ord_details.t01_routingac AS portfolio_no,
                   ord_details.t01_cumqty AS order_qty,
                   ord_details.t01_lastpx AS price,
                   ord_details.t01_symbol AS symbol,
                   ord_details.open AS ltp,
                   CASE
                       WHEN ord_details.t01_side = 1 THEN 'Buy'
                       WHEN ord_details.t01_side = 2 THEN 'Sell'
                   END
                       AS ord_side,
                   ROUND (
                         (ord_details.t01_lastpx - ord_details.open)
                       * 100
                       / ord_details.t01_lastpx,
                       2)
                       AS pct_change,
                   ord_details.t01_m01_customer_id,
                   TRIM (
                          TRIM (
                                 TRIM (
                                        NVL (m01.m01_c1_other_names, '')
                                     || ' '
                                     || NVL (m01.m01_c1_last_name, ''))
                              || ' '
                              || NVL (m01.m01_c1_second_name, ''))
                       || ' '
                       || NVL (m01.m01_c1_third_name, ''))
                       AS customer_name,
                   ROUND (ord_details.t01_lastpx * ord_details.t01_cumqty)
                       AS order_value
              FROM m01_customer m01,
                   (SELECT *
                      FROM mubasher_price.esp_intraday_ohlc ohlc,
                           (SELECT *
                              FROM t01_order_summary_intraday_all b
                             WHERE     b.t01_exchange IN
                                           (SELECT m11_exchangecode
                                              FROM m11_exchanges
                                             WHERE m11_is_local = 1)
                                   AND TO_NUMBER (
                                           TO_CHAR (b.t01_last_updated,
                                                    'hh24')) = 12
                                   AND TO_NUMBER (
                                           TO_CHAR (b.t01_last_updated, 'MI')) =
                                           29
                                   AND (   (b.t01_ordstatus IN
                                                ('1', '2', 'q', 'r'))
                                        OR (    b.t01_ordstatus IN
                                                    ('5', '4', 'C', '8', '9')
                                            AND b.t01_cumqty > 0))
                                   AND b.t01_last_updated BETWEEN TO_DATE (
                                                                      pdate)
                                                              AND   TO_DATE (
                                                                        pdate)
                                                                  + 0.99999
                                   AND b.t01_lastpx <> b.t01_price) t01
                     WHERE     ohlc.symbol = t01.t01_symbol
                           AND ohlc.trade_min > t01.t01_last_updated
                           AND ohlc.close = t01.t01_lastpx
                           AND ABS (
                                     (t01.t01_lastpx - ohlc.open)
                                   / t01.t01_lastpx) >= 0.01) ord_details
             WHERE ord_details.t01_m01_customer_id = m01.m01_customer_id;
    END;

    PROCEDURE sp_get_adv_portfolio_summery (
        pview                 OUT refcursor,
        pt04_security_ac_id       NUMBER,
        pt04_exchange             VARCHAR2,
        pfrom_date                DATE,
        pto_date                  DATE)
    IS
        l_u05_cash_account_id        u05_security_accounts.u05_cash_account_id%TYPE;
        l_u05_prefered_cost_method   u05_security_accounts.u05_prefered_cost_method%TYPE;
        l_u05_portfolio_name         u05_security_accounts.u05_portfolio_name%TYPE;
    BEGIN
        SELECT u05_cash_account_id,
               u05_prefered_cost_method,
               u05_portfolio_name
          INTO l_u05_cash_account_id,
               l_u05_prefered_cost_method,
               l_u05_portfolio_name
          FROM u05_security_accounts
         WHERE u05_id = pt04_security_ac_id;

        OPEN pview FOR
            SELECT holdings.trans_date,
                   holdings.security_acc_id,
                   holdings.exchange_code,
                   holdings.symbol_code,
                   holdings.net_holdings,
                   holdings.pledged_qty,
                   holdings.sell_pending,
                   holdings.avg_cost,
                   holdings.avg_price,
                   holdings.market_value,
                   cash.account_balance,
                   cash.blocked_amount,
                   cash.tot_sell_amt,
                   ABS (cash.tot_buy_amt) AS tot_buy_amt,
                   cash.tot_depost_amt,
                   cash.tot_withdr_amt,
                   cash.tot_commision,
                   holdings.u05_prefered_cost_method,
                   holdings.m77_short_description_en,
                   holdings.m77_short_description_other
              FROM (SELECT TRUNC (SYSDATE) AS trans_date,
                           t04_security_ac_id AS security_acc_id,
                           t04_exchange AS exchange_code,
                           t04_symbol AS symbol_code,
                           (  t04_net_holdings
                            + t04_payable_holding
                            - t04_pending_settle
                            - t04_other_blocked_qty)
                               AS net_holdings,
                           t04_pledgedqty AS pledged_qty,
                           t04_sell_pending AS sell_pending,
                           (CASE
                                WHEN l_u05_prefered_cost_method            --1
                                                               = 2
                                THEN
                                    t04_weighted_avg_cost
                                ELSE
                                    t04_avg_cost
                            END)
                               AS avg_cost,
                           t04_avg_price AS avg_price,
                           l_u05_prefered_cost_method                      --1
                               AS u05_prefered_cost_method,
                             NVL (esp.market_price, 0)
                           * (  t04_net_holdings
                              + t04_payable_holding
                              - t04_pending_settle
                              - t04_other_blocked_qty)
                               AS market_value,
                           l_u05_portfolio_name AS u05_portfolio_name,
                           m77.m77_short_description_en,
                           m77.m77_short_description_other
                      FROM t04_holdings_intraday t04,
                           vw_dc_esp_todays_snapshots esp,
                           m77_symbols m77
                     WHERE     t04.t04_security_ac_id = pt04_security_ac_id --21665
                           AND (  t04.t04_net_holdings
                                + t04.t04_pending_settle
                                + t04.t04_pledgedqty) <> 0
                           AND t04.t04_exchange = 'TDWL'
                           AND t04.t04_exchange = esp.exchangecode(+)
                           AND t04.t04_symbol = esp.symbol(+)
                           AND t04.t04_symbol = m77.m77_symbol(+)
                           AND t04.t04_exchange = m77.m77_exchange(+)
                           AND TRUNC (SYSDATE) = TRUNC (pfrom_date) --TRUNC (SYSDATE)
                    UNION ALL
                    SELECT s01_trimdate AS trans_date,
                           s01_security_ac_id AS security_acc_id,
                           s01_exchange AS exchange_code,
                           s01_symbol AS symbol_code,
                           (  s01_net_holdings
                            + s01_payable_holding
                            - s01_pending_settle
                            - s01_other_blocked_qty)
                               AS net_holdings,
                           s01_pledgedqty AS pledged_qty,
                           s01_sell_pending AS sell_pending,
                           (CASE
                                WHEN l_u05_prefered_cost_method            --1
                                                               = 2
                                THEN
                                    s01_weighted_avg_cost
                                ELSE
                                    s01_avg_cost
                            END)
                               AS avg_cost,
                           s01_avg_price AS avg_price,
                           l_u05_prefered_cost_method                      --1
                               AS u05_prefered_cost_method,
                             NVL (esp.close, 0)
                           * (  s01_net_holdings
                              + s01_payable_holding
                              - s01_pending_settle
                              - s01_other_blocked_qty)
                               AS market_value,
                           l_u05_portfolio_name AS u05_portfolio_name,
                           m77.m77_short_description_en,
                           m77.m77_short_description_other
                      FROM s01_holdings_summary_extnd_all s01,
                           vw_dc_esp_transactions_complt esp,
                           m77_symbols m77
                     WHERE     s01.s01_security_ac_id = pt04_security_ac_id --21665
                           AND s01.s01_exchange = 'TDWL'
                           AND s01.s01_exchange = esp.exchangecode(+)
                           AND s01.s01_symbol = esp.symbol(+)
                           AND s01.s01_net_holdings <> 0
                           AND s01.s01_symbol = m77.m77_symbol(+)
                           AND s01.s01_exchange = m77.m77_exchange(+)
                           AND s01_trimdate = TRUNC (pfrom_date)
                           AND esp.transactiondate = TRUNC (pfrom_date)) holdings,
                   (SELECT acc_bal.trans_date,
                           account_balance AS account_balance,
                           blocked_amount AS blocked_amount,
                           NVL (tot_sell_amt, 0) AS tot_sell_amt,
                           NVL (tot_buy_amt, 0) AS tot_buy_amt,
                           NVL (tot_depost_amt, 0) AS tot_depost_amt,
                           NVL (tot_withdr_amt, 0) AS tot_withdr_amt,
                           NVL (tot_commision, 0) AS tot_commision
                      FROM (SELECT trans_date,
                                   balance AS account_balance,
                                   blocked_amount AS blocked_amount
                              FROM (SELECT s02_trimdate AS trans_date,
                                           s02_balance AS balance,
                                           s02_blocked_amount
                                               AS blocked_amount
                                      FROM s02_cash_account_sum_extnd_all
                                     WHERE     s02_account_id =
                                                   l_u05_cash_account_id --21665
                                           AND s02_trimdate =
                                                   TRUNC (pfrom_date) --TRUNC (SYSDATE)
                                    UNION ALL
                                    SELECT TRUNC (SYSDATE) AS trans_date,
                                           t03_balance AS balance,
                                           t03_blocked_amount
                                               AS blocked_amount
                                      FROM t03_cash_account
                                     WHERE     t03_account_id =
                                                   l_u05_cash_account_id --21665
                                           AND TRUNC (SYSDATE) =
                                                   TRUNC (pfrom_date)) --TRUNC (SYSDATE)
                                                                      ) acc_bal,
                           (SELECT t05_date,
                                   SUM (
                                       DECODE (t05_code,
                                               'STLSEL', t05_amount,
                                               0))
                                       AS tot_sell_amt,
                                   SUM (
                                       DECODE (t05_code,
                                               'STLBUY', t05_amount,
                                               0))
                                       AS tot_buy_amt,
                                   SUM (
                                       DECODE (t05_code,
                                               'DEPOST', t05_amount,
                                               0))
                                       AS tot_depost_amt,
                                   SUM (
                                       DECODE (t05_code,
                                               'WITHDR', t05_amount,
                                               0))
                                       AS tot_withdr_amt,
                                   SUM (t05_commission) AS tot_commision
                              FROM (SELECT TRUNC (pfrom_date) AS t05_date,
                                           t05_code,
                                           ROUND (
                                                 t05_amt_in_trans_currency
                                               * (CASE
                                                      WHEN t05_trans_base_curr_rate >
                                                               0
                                                      THEN
                                                          t05_trans_base_curr_rate
                                                      ELSE
                                                          1
                                                  END),
                                               2)
                                               AS t05_amount,
                                           ROUND (t05_commission, 2)
                                               AS t05_commission
                                      FROM t05_cash_account_log
                                     WHERE     t05_cash_account_id =
                                                   l_u05_cash_account_id --21665
                                           AND t05_date BETWEEN TRUNC (
                                                                    pfrom_date) --TRUNC (SYSDATE)
                                                            AND   TRUNC (
                                                                      pfrom_date)
                                                                + .99999 --TRUNC (SYSDATE)
                                           AND t05_code IN
                                                   ('STLSEL',
                                                    'STLBUY',
                                                    'DEPOST',
                                                    'WITHDR'))) txn
                     WHERE acc_bal.trans_date = txn.t05_date(+)) cash
             WHERE holdings.trans_date = cash.trans_date;
    END;

    PROCEDURE sp_sg_fcg_internal_revenue (pview             OUT refcursor,
                                          prowcnt           OUT INT,
                                          psortcolumn           VARCHAR2,
                                          pn1                   INT,
                                          pn2                   INT,
                                          psearchcriteria       VARCHAR2,
                                          pd1                   DATE,
                                          pd2                   DATE,
                                          pcurrancy             VARCHAR2)
    IS
        l_qry   VARCHAR2 (15000);
    BEGIN
        l_qry :=
               'SELECT   m87_description as cost_center_code,
                m01_full_name as clients,
                SUM(abs(t05_amt_in_settle_currency
                    * get_exchange_rate (t03.t03_branch_id, t03.t03_currency, '''
            || pcurrancy
            || ''')))
                     AS gross_amount,
                SUM(ABS(t05_commission
                   * get_exchange_rate (
                         t03.t03_branch_id,
                         t03.t03_currency,
                         '''
            || pcurrancy
            || '''
                     )))
                    AS gross_commission,
                SUM(ABS(t05_exg_commission
                   * get_exchange_rate (
                         t03.t03_branch_id,
                         t03.t03_currency,
                        '''
            || pcurrancy
            || '''
                     )))
                    AS exchange_charges,
                SUM(ABS( (t05_commission - t05_exg_commission)
                   * get_exchange_rate (
                         t03.t03_branch_id,
                         t03.t03_currency,
                         '''
            || pcurrancy
            || '''
                     )))
                    AS net_commission,
                m01_customer_id,
                TO_DATE ('''
            || TO_CHAR (pd2, 'DD-MM-YYYY')
            || ''',
                              ''DD-MM-YYYY'')
                         AS t05_date
            FROM   m01_customer m01,
                t05_cash_account_log t05,
                t03_cash_account t03,
                m87_cost_centers m87
         WHERE       m01.m01_cost_center = m87.m87_id(+)
                 AND t05.t05_cash_account_id = t03.t03_account_id
                AND t03.t03_profile_id = m01.m01_customer_id
                AND t05_date BETWEEN TO_DATE (
                                    '''
            || TO_CHAR (pd1, 'DD-MM-YYYY')
            || ''',
                                    ''DD-MM-YYYY''
                                )
                            AND  TO_DATE (
                                     '''
            || TO_CHAR (pd2, 'DD-MM-YYYY')
            || ''',
                                     ''DD-MM-YYYY''
                                 )
                                 + .99999
        GROUP BY   m01_customer_id, m87_description, m01_full_name
        ORDER BY   m87_description';

        IF (pn1 = 1)
        THEN
            EXECUTE IMMEDIATE
                   'SELECT COUNT ( * ) FROM ('
                || l_qry
                || ')'
                || CASE
                       WHEN psearchcriteria IS NOT NULL
                       THEN
                           ' WHERE ' || psearchcriteria
                       ELSE
                           ''
                   END
                INTO prowcnt;
        ELSE
            prowcnt := -2;
        END IF;

        IF psortcolumn IS NOT NULL
        THEN
            OPEN pview FOR
                   'SELECT t2.* FROM (SELECT t1.*, ROWNUM rnum FROM (SELECT t3.*, ROW_NUMBER() OVER(ORDER BY '
                || psortcolumn
                || ') runm FROM ('
                || l_qry
                || ') t3'
                || ' WHERE '
                || psearchcriteria
                || ') t1 WHERE ROWNUM <= '
                || pn2
                || ') t2 WHERE RNUM >= '
                || pn1;
        ELSE
            OPEN pview FOR
                   'SELECT t2.* FROM (SELECT t1.*, ROWNUM rn FROM (SELECT * FROM ('
                || l_qry
                || ') WHERE '
                || psearchcriteria
                || ') t1 WHERE ROWNUM <= '
                || pn2
                || ') t2 WHERE rn >= '
                || pn1;
        END IF;
    END;

    PROCEDURE sp_exchange_settlement_rpt (pview OUT refcursor, pdate DATE)
    AS
    BEGIN
        OPEN pview FOR
              SELECT m01_customer_id,
                     record_type,
                     custname,
                     exg_commission,
                     total_buy,
                     total_sell,
                     (total_buy - total_sell) AS net_trade,
                     (total_buy - total_sell + exg_commission)
                         AS net_obligation,
                     custname_ar,
                     exchange
                FROM (  SELECT m01_customer_id,
                               CASE
                                   WHEN t03_exchange_account_type = 0
                                   THEN
                                       'Customer'
                                   ELSE
                                       'SWAP Customer'
                               END
                                   AS record_type,
                               m01.m01_full_name AS custname,
                               ABS (SUM (t05.t05_exg_commission))
                                   AS exg_commission,
                               ABS (
                                   SUM (
                                       CASE
                                           WHEN t05.t05_code IN
                                                    ('STLBUY', 'REVBUY')
                                           THEN
                                               t05.t05_amount
                                           ELSE
                                               0
                                       END))
                                   AS total_buy,
                               ABS (
                                   SUM (
                                       CASE
                                           WHEN t05.t05_code IN
                                                    ('STLSEL', 'REVSEL')
                                           THEN
                                               t05.t05_amount
                                           ELSE
                                               0
                                       END))
                                   AS total_sell,
                                  TRIM (
                                         NVL (m01.m01_c1_arabic_first_name, '')
                                      || ' '
                                      || NVL (m01.m01_c1_arabic_second_name, ''))
                               || ' '
                               || NVL (m01.m01_c1_arabic_third_name, '')
                                   AS custname_ar,
                               t05.t05_exchange AS exchange
                          FROM t05_cash_account_log t05,
                               t03_cash_account t03,
                               m01_customer m01
                         WHERE     t05.t05_cash_account_id = t03.t03_account_id
                               AND t03.t03_profile_id = m01.m01_customer_id
                               AND t05.t05_code IN
                                       ('STLBUY', 'STLSEL', 'REVBUY', 'REVSEL')
                               AND TRUNC (t05.t05_date) = TRUNC (pdate)
                      GROUP BY m01_customer_id,
                               t03_exchange_account_type,
                               m01.m01_full_name,
                               t05_exchange,
                               m01_c1_arabic_first_name,
                               m01_c1_arabic_second_name,
                               m01_c1_arabic_third_name)
            ORDER BY m01_customer_id;
    END;

    PROCEDURE sp_commission_report (pview       OUT refcursor,
                                    pfromdate       DATE,
                                    ptodate         DATE,
                                    pinst_id        NUMBER,
                                    pcurrency       VARCHAR2)
    IS
    BEGIN
        OPEN pview FOR
            SELECT a.t01_clordid,
                   a.t01_createddate,
                   a.t01_last_updated,
                   a.t01_symbol,
                   a.t01_exchange,
                   a.t01_currency,
                   a.t01_price,
                   a.t01_lastpx,
                   a.t01_avgpx,
                   a.t01_orderqty,
                   a.t01_cumnetsettle,
                   a.t01_commission,
                   a.t01_ordvalue,
                   a.t01_ordnetvalue,
                   a.t01_cumqty,
                   a.t01_side,
                   a.m16_descriotion_1,
                   a.m14_description_1,
                   a.m13_description_1,
                   a.u05_accountno,
                   a.u05_id,
                   a.m01_customer_id,
                   a.m01_c1_customer_id,
                   a.custname,
                   a.t03_accountno,
                   a.t03_branch_id,
                   a.u05_customer_id,
                   a.t01_channel,
                   a.m44_description,
                   a.t01_ordstatus,
                   a.t01_userid,
                   NVL (a.dealername, 'SYSTEM') AS dealername,
                   TRUNC (a.t01_createddate) AS trim_date,
                   CASE
                       WHEN a.t01_currency = pcurrency
                       THEN
                           1
                       WHEN (a.t01_currency <> pcurrency AND a.t01_side = 1)
                       THEN
                           NVL (exchange_rates_all.m03_sell_rate, 1)
                       WHEN (a.t01_currency <> pcurrency AND a.t01_side = 2)
                       THEN
                           NVL (exchange_rates_all.m03_buy_rate, 1)
                       ELSE
                           1
                   END
                       AS rate
              FROM t01_order_summary_intraday_v a,
                   (SELECT b.m03_date,
                           b.m03_c1,
                           b.m03_c2,
                           b.m03_rate,
                           m03_buy_rate,
                           m03_sell_rate
                      FROM vw_dc_m03_exchange_rates_base b
                     WHERE     b.m03_date >= TRUNC (pfromdate)
                           AND b.m03_inst_id = pinst_id
                           AND b.m03_c2 = pcurrency) exchange_rates_all
             WHERE     TRUNC (a.t01_createddate) =
                           exchange_rates_all.m03_date(+)
                   AND a.t01_currency = exchange_rates_all.m03_c1(+)
                   AND a.t01_createddate BETWEEN TRUNC (pfromdate)
                                             AND (TRUNC (ptodate) + 1)
                   AND a.t01_ordstatus IN ('1', '2', 'C', '4')
                   AND a.t01_cumqty > 0
                   AND a.t03_branch_id = pinst_id;
    END;

    PROCEDURE sp_commission_breakdown_report (pview       OUT refcursor,
                                              pfromdate       DATE,
                                              ptodate         DATE,
                                              pexchange       VARCHAR2,
                                              pcurrency       VARCHAR2)
    IS
    BEGIN
        IF pexchange = 'All'
        THEN
            OPEN pview FOR
                  SELECT t03.t03_m01_full_name,
                         t01.t01_portfoliono,
                         t03_m01_c1_customer_id,
                         t05.t05_orderno,
                         t05.t05_symbol,
                         m13.m13_description_1,
                         t05.t05_exchange,
                         MAX (t05.t05_timestamp) AS t05_timestamp,
                           SUM (t05.t05_commission)
                         * get_exchange_rate (m01.m01_locations_id,
                                              t05.t05_transaction_currency,
                                              pcurrency)
                             AS total_commission,
                           SUM (t05.t05_exg_commission)
                         * get_exchange_rate (m01.m01_locations_id,
                                              t05.t05_transaction_currency,
                                              pcurrency)
                             AS exchange_commission,
                           SUM ( (t05.t05_commission - t05.t05_exg_commission))
                         * get_exchange_rate (m01.m01_locations_id,
                                              t05.t05_transaction_currency,
                                              pcurrency)
                             brokercommission
                    FROM t05_cash_account_log t05,
                         t03_cash_account t03,
                         t01_order_summary_intraday t01,
                         m13_order_side m13,
                         m01_customer m01
                   WHERE     t05.t05_cash_account_id = t03.t03_account_id
                         AND t05.t05_orderno = t01.t01_orderno
                         AND t01.t01_side = m13.m13_side_id
                         AND t03.t03_m01_c1_customer_id =
                                 m01.m01_c1_customer_id
                         AND (TRUNC (t05.t05_date) BETWEEN (pfromdate)
                                                       AND (ptodate))
                GROUP BY t03.t03_m01_full_name,
                         t01.t01_portfoliono,
                         t05.t05_orderno,
                         t05.t05_exchange,
                         t05.t05_symbol,
                         t03_m01_c1_customer_id,
                         m13.m13_description_1,
                         m01.m01_locations_id,
                         t05.t05_transaction_currency
                ORDER BY t03.t03_m01_c1_customer_id;
        ELSE
            OPEN pview FOR
                  SELECT t03.t03_m01_full_name,
                         t01.t01_portfoliono,
                         t03_m01_c1_customer_id,
                         t05.t05_orderno,
                         t05.t05_symbol,
                         m13.m13_description_1,
                         t05.t05_exchange,
                         MAX (t05.t05_timestamp) AS t05_timestamp,
                           SUM (t05.t05_commission)
                         * get_exchange_rate (m01.m01_locations_id,
                                              t05.t05_transaction_currency,
                                              pcurrency)
                             AS total_commission,
                           SUM (t05.t05_exg_commission)
                         * get_exchange_rate (m01.m01_locations_id,
                                              t05.t05_transaction_currency,
                                              pcurrency)
                             AS exchange_commission,
                           SUM ( (t05.t05_commission - t05.t05_exg_commission))
                         * get_exchange_rate (m01.m01_locations_id,
                                              t05.t05_transaction_currency,
                                              pcurrency)
                             brokercommission
                    FROM t05_cash_account_log t05,
                         t03_cash_account t03,
                         t01_order_summary_intraday t01,
                         m13_order_side m13,
                         m01_customer m01
                   WHERE     t05.t05_cash_account_id = t03.t03_account_id
                         AND t05.t05_orderno = t01.t01_orderno
                         AND t01.t01_side = m13.m13_side_id
                         AND t03.t03_m01_c1_customer_id =
                                 m01.m01_c1_customer_id
                         AND t05_exchange = pexchange
                         AND (TRUNC (t05.t05_date) BETWEEN (pfromdate)
                                                       AND (ptodate))
                GROUP BY t03.t03_m01_full_name,
                         t01.t01_portfoliono,
                         t05.t05_orderno,
                         t05.t05_exchange,
                         t05.t05_symbol,
                         t03_m01_c1_customer_id,
                         m13.m13_description_1,
                         m01.m01_locations_id,
                         t05.t05_transaction_currency
                ORDER BY t03.t03_m01_c1_customer_id;
        END IF;
    END;

    PROCEDURE sp_t12_pending_cash_summary (
        pview             OUT refcursor,
        prowcnt           OUT INT,
        psortcolumn           VARCHAR2,
        pn1                   INT,
        pn2                   INT,
        psearchcriteria       VARCHAR2,
        ptstatus              VARCHAR2 DEFAULT '0,1,2,4,6')
    IS
        l_qry   VARCHAR2 (15000);
    BEGIN
        l_qry :=
               'SELECT   TRUNC (t12_date) AS tdate,
                           t12_transaction_currency AS currency,
                           m05_branch_id,
                           m05_branch_code,
                           SUM(CASE
                                   WHEN     t12_code = ''DEPOST''
                                        AND t12_approved2_by IS NULL AND t12_status IN ('
            || ptstatus
            || ')
                                   THEN
                                       t12_amt_in_trans_currency
                               END)
                               AS pending_deposits,
                           SUM(CASE
                                   WHEN     t12_code = ''WITHDR''
                                        AND t12_approved2_by IS NULL  AND t12_status IN ('
            || ptstatus
            || ')
                                   THEN
                                       -t12_amt_in_trans_currency
                               END)
                               AS pending_withdrawals,
                           SUM(CASE
                                   WHEN     t12_code IN (''DEPOST'', ''WITHDR'')
                                        AND t12_approved2_by IS NULL AND t12_status IN ('
            || ptstatus
            || ')
                                   THEN
                                       t12_amt_in_trans_currency
                               END)
                               AS pending_total,
                           SUM(CASE
                                   WHEN t12_code = ''DEPOST''
                                        AND t12_approved2_by IS NOT NULL
                                   THEN
                                       t12_amt_in_trans_currency
                               END)
                               AS approved_deposits,
                           SUM(CASE
                                   WHEN t12_code = ''WITHDR''
                                        AND t12_approved2_by IS NOT NULL
                                   THEN
                                       -t12_amt_in_trans_currency
                               END)
                               AS approved_withdrawals,
                           SUM(CASE
                                   WHEN t12_code IN (''DEPOST'', ''WITHDR'')
                                        AND t12_approved2_by IS NOT NULL
                                   THEN
                                       t12_amt_in_trans_currency
                               END)
                               AS approved_total,
                           SUM(CASE
                                   WHEN t12_code = ''DEPOST''
                                   THEN
                                       t12_amt_in_trans_currency
                               END)
                               AS deposits,
                           SUM(CASE
                                   WHEN t12_code = ''WITHDR''
                                   THEN
                                       -t12_amt_in_trans_currency
                               END)
                               AS withdrawals,
                           SUM(CASE
                                   WHEN t12_code IN (''DEPOST'', ''WITHDR'')
                                   THEN
                                       t12_amt_in_trans_currency
                               END)
                               AS total
                    FROM   t12_pending_cash_all
                    WHERE  t12_code IN (''DEPOST'', ''WITHDR'')
                GROUP BY   TRUNC (t12_date),
                           t12_transaction_currency,
                           m05_branch_id,
                           m05_branch_code';


        IF (pn1 = 1)
        THEN
            EXECUTE IMMEDIATE
                   'SELECT COUNT ( * ) FROM ('
                || l_qry
                || ')'
                || CASE
                       WHEN psearchcriteria IS NOT NULL
                       THEN
                           ' WHERE ' || psearchcriteria
                       ELSE
                           ''
                   END
                INTO prowcnt;
        ELSE
            prowcnt := -2;
        END IF;

        OPEN pview FOR
               'SELECT t2.* FROM (SELECT t1.*, ROWNUM rn FROM (SELECT * FROM ('
            || l_qry
            || ') WHERE '
            || psearchcriteria
            || ') t1 WHERE ROWNUM <= '
            || pn2
            || ') t2 WHERE rn >= '
            || pn1;
    END;
END;
/

