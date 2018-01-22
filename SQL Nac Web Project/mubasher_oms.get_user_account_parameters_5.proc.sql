CREATE OR REPLACE PROCEDURE mubasher_oms.get_user_account_parameters_5 (
    security_ac_no   IN     VARCHAR2,
    parms               OUT VARCHAR2)
IS
    inst_id                       NUMBER;
    sec_acc_id                    NUMBER;
    cash_acc_id                   NUMBER;
    customer_id                   NUMBER;
    status                        NUMBER;
    acc_type                      NUMBER;
    is_sub_account                NUMBER;                -- 0 - normal 1 - sub
    parent_id                     NUMBER;
    parent_sec_acc_id             NUMBER;
    parent_sec_acc_no             VARCHAR2 (20);
    routing_ac_type               INT;
    routing_ac                    VARCHAR2 (30);
    is_custodian_acc              NUMBER;
    mubasherno                    VARCHAR2 (10);
    sharicomplient                INT;
    sharicomplient_parent         INT;
    sexchange                     VARCHAR2 (30);
    comm_cat                      INT;
    comm_group_id                 INT;
    margin_enabled                INT;
    portfolio_name                VARCHAR2 (255);
    marginmaxamount               NUMBER (18, 5);
    daymarginmaxamount            NUMBER (18, 5);
    acc_category                  m01_customer.m01_acc_category%TYPE;
    phone_identification_code     m01_customer.m01_phone_identification_code%TYPE;
    rebate_group_id               INT;
    currency                      VARCHAR2 (20);
    cash_acc_no                   VARCHAR2 (20);
    investment_acc_no             VARCHAR2 (50);
    forex_settle_type             NUMBER;
    l_u05_security_account_type   NUMBER;

    CURSOR rout_acc_cursor
    IS
        (SELECT (CASE
                     WHEN u06_comm_disc_appl_per = 0
                     THEN
                         NVL (m288.m288_discount, 0)
                     ELSE
                         u06_comm_disc_appl_per
                 END)
                    AS u06_comm_disc_appl_per,
                u06_stock_transaction_restrict,
                u06_exchange,
                u06_routing_ac_type,
                DECODE (u06_routing_ac_type,
                        0, u06_exchange_ac,
                        1, u06_omini_ac_id)
                    AS routing_ac,
                u06_bank_ac_id,
                u06_omini_ac_id,
                u06_commision_type,
                u06_executing_broker_ref,
                u06_commision_amt_small,
                u06_is_custodian_account,
                u06_commision_share,
                u06_txn_fee,
                u06_sharia_complient,
                u06_commision_group_id,
                u06_cond_orders,
                u06_symbol_cond,
                u06_daydiscount,
                u06_exec_broker_inst,
                u06_exec_broker_rebate,
                u06_custodian_inst_id,
                u06_allow_all_channals,
                u06_commsion_discount_group_id,
                u06_stock_transaction_rtc_narr,
                u06_stock_transfer_disabled,
                u06_stock_transfer_disabled_na,
                u06_trading_restriction,
                u06_trading_restriction_narr,
                u06_pledge_restriction,
                u06_pledge_restriction_narr,
                u06_trading_enabled,
                u06_discount_segment,
                u06_depositor_ac,
                u06_secondary_commission_group,
                u06_comm_group_type,
                u06_exec_broker_sid,
                u06_invester_ac_no,
                u06_concentration_group,
                u06_min_order_qty,
                u06_sharia_complient_grp,
                u06_market_segment
           FROM u06_routing_accounts u06, m288_discount_segments m288
          WHERE     m288.m288_id(+) = u06.u06_discount_segment
                AND u06_security_ac_id = sec_acc_id);

    cur_var                       rout_acc_cursor%ROWTYPE;

    CURSOR commision_acc_cursor
    IS
        (SELECT m52_from,
                m52_to,
                m52_percentage,
                m52_flat_amount,
                m52_comm_type,
                m52_min_comm,
                m52_max_comm,
                m52_currency,
                m52_service,
                m52_instrument_type
           FROM m52_commission_structures_v m52
          WHERE m52_commission_group_id = comm_group_id);

    comm_var                      commision_acc_cursor%ROWTYPE;
    allow_channels                NUMBER (1);

    CURSOR restricted_channel_cursor (sexchange IN VARCHAR2)
    IS
        (SELECT u20_order_channel, u20_type
           FROM u20_restricted_channels
          WHERE u20_security_ac_id = sec_acc_id AND u20_exchange = sexchange);

    restricted_channel_var        restricted_channel_cursor%ROWTYPE;

    CURSOR cust_rebate_cursor
    IS
        (SELECT a.m82_from_value,
                a.m82_to_value,
                a.m82_percentage,
                a.m82_flat_amount,
                a.m82_currency
           FROM m82_customer_rebate_structs a
          WHERE a.m82_rebate_group = rebate_group_id);

    rebate_var                    cust_rebate_cursor%ROWTYPE;


    CURSOR symbol_restriction_cursor (
        customer_id       IN NUMBER,
        security_acc_id   IN NUMBER,
        symbol_exchange   IN VARCHAR2)
    IS
        (SELECT u08_symbol, u08_type
           FROM u08_restricted_symbols
          WHERE     u08_customer_id = customer_id
                AND u08_security_ac_id = security_acc_id
                AND u08_exchangecode = symbol_exchange);

    symbol_restriction_var        symbol_restriction_cursor%ROWTYPE;


    CURSOR instrument_restrict_cursor (
        customer_id       IN NUMBER,
        security_acc_id   IN NUMBER,
        symbol_exchange   IN VARCHAR2)
    IS
        (SELECT a.u10_instrument_type_id, a.u10_exchange
           FROM u10_restricted_instrmnt_types a
          WHERE     u10_customer_id = customer_id
                AND u10_security_ac_id = security_acc_id
                AND u10_exchange = symbol_exchange);

    instrument_restrict_var       instrument_restrict_cursor%ROWTYPE;

    CURSOR sector_restrict_cursor (
        customer_id       IN NUMBER,
        security_acc_id   IN NUMBER,
        symbol_exchange   IN VARCHAR2)
    IS
        (SELECT a.u09_sectorcode, a.u09_exchangecode
           FROM u09_restricted_sectors a
          WHERE     u09_customer_id = customer_id
                AND u09_security_ac_id = security_acc_id
                AND u09_exchangecode = symbol_exchange);

    sector_restrict_var           sector_restrict_cursor%ROWTYPE;

    --egypt
    CURSOR custodian_cursor
    IS
        (SELECT a.m103_security_ac_id,
                a.m103_exchange,
                a.m103_custodian,
                a.m103_is_default,
                b.ex01_name,
                b.ex01_sid,
                b.ex01_id
           FROM m103_exchange_acc_custodian a, ex01_executing_institution b
          WHERE     a.m103_security_ac_id = sec_acc_id
                AND b.ex01_id = a.m103_custodian);

    custodian_var                 custodian_cursor%ROWTYPE;
BEGIN
    parent_id := -1;
    parent_sec_acc_id := -1;
    parent_sec_acc_no := '*';
    is_sub_account := 0;

    -- Get customer number which is also profile no
    SELECT u05_id,
           u05_cash_account_id,
           u05_customer_id,
           u05_status_id,
           u05_type,
           u05_account_type,
           u05_branch_id,
           u05_shariacomplient,
           u05_portfolio_name,                         /*u05_mrg_trd_enabled*/
           DECODE (u05_mrg_day_trd_enabled,
                   1, u05_mrg_day_trd_enabled,
                   u05_mrg_trd_enabled),
           NVL (u05_parent_ac_id, 0),
           u05_mrg_trd_max_amt,
           u05_day_trd_max_amt,
           t03_currency,
           t03_accountno,
           t03_investor_acc_no,
           t03_forex_settle_type,
           u05_security_account_type
      INTO sec_acc_id,
           cash_acc_id,
           customer_id,
           status,
           acc_type,
           is_sub_account,
           inst_id,
           sharicomplient,
           portfolio_name,
           margin_enabled,
           parent_sec_acc_id,
           marginmaxamount,
           daymarginmaxamount,
           currency,
           cash_acc_no,
           investment_acc_no,
           forex_settle_type,
           l_u05_security_account_type
      FROM u05_security_accounts, t03_cash_account
     WHERE     u05_accountno = security_ac_no
           AND t03_account_id = u05_cash_account_id
           AND u05_status_id NOT IN (5, 21);



    SELECT m01_c1_customer_id,
           m01_parent_ac_id                                             -- MNM
                           ,
           m01_acc_category,
           m01_phone_identification_code,
           m73_rebate_group
      -- MNM
      INTO mubasherno,
           parent_id                                                    -- MNM
                    ,
           acc_category,
           phone_identification_code,
           rebate_group_id
      -- MNM
      FROM m01_customer, m73_customer_groups
     WHERE m01_customer_id = customer_id AND m01_customer_group = m73_id;

    IF (parent_sec_acc_id > 0)
    THEN
        SELECT u05_accountno, u05_shariacomplient
          INTO parent_sec_acc_no, sharicomplient_parent
          FROM u05_security_accounts
         WHERE u05_id = parent_sec_acc_id;
    END IF;

    -- MNM In saudi code instead of parent_id parent_sec_acc_no
    parms :=
           sec_acc_id
        || ','
        || security_ac_no
        || ','
        || cash_acc_id
        || ','
        || customer_id
        || ','
        || status
        || ','
        || acc_type
        || ','
        || is_sub_account
        || ','
        || NVL (parent_id, 0)
        || ','
        || NVL (parent_sec_acc_id, 0)
        || ','
        || inst_id
        || ','
        || NVL (mubasherno, '*')
        || ','
        || NVL (portfolio_name, security_ac_no)
        || ','
        || NVL (margin_enabled, 0)                                         --;
        -- MNM
        || ','
        || NVL (acc_category, 0)
        || ','
        || NVL (phone_identification_code, '*')
        -- MNM
        || ','
        || NVL (marginmaxamount, 0)
        || ','
        || NVL (daymarginmaxamount, 0)
        || ','
        || NVL (currency, '*')
        || ','
        || cash_acc_no
        || ','
        || NVL (investment_acc_no, '*')
        || ','
        || NVL (forex_settle_type, 0)
        || ','
        || NVL (l_u05_security_account_type, 1);

    OPEN custodian_cursor;

    FETCH custodian_cursor INTO custodian_var;

    WHILE custodian_cursor%FOUND
    LOOP
        parms :=
               parms
            || ','
            || custodian_var.ex01_id
            || ','
            || custodian_var.ex01_sid
            || ','
            || custodian_var.ex01_name
            || ','
            || custodian_var.m103_is_default
            || ','
            || custodian_var.m103_exchange;

        FETCH custodian_cursor INTO custodian_var;
    END LOOP;

    CLOSE custodian_cursor;

    OPEN rout_acc_cursor;

    FETCH rout_acc_cursor INTO cur_var;

    WHILE rout_acc_cursor%FOUND
    LOOP
        -- MNM
        routing_ac_type := cur_var.u06_routing_ac_type;

        DBMS_OUTPUT.put_line (cur_var.u06_exchange);

        IF (routing_ac_type = 1)
        THEN
            SELECT m25_exchange_ac
              INTO routing_ac
              FROM m25_omi_accounts
             WHERE m25_id = cur_var.u06_omini_ac_id;
        ELSE
            IF (routing_ac_type = 2)
            THEN
                DBMS_OUTPUT.put_line ('hellonew');
                DBMS_OUTPUT.put_line (parent_sec_acc_id);
                DBMS_OUTPUT.put_line (cur_var.u06_exchange);

                SELECT u06_exchange_ac, u06_sharia_complient
                  INTO routing_ac, sharicomplient
                  FROM u06_routing_accounts
                 WHERE     u06_security_ac_id = parent_sec_acc_id
                       AND u06_exchange = cur_var.u06_exchange;
            ELSE
                -- MNM
                routing_ac := cur_var.routing_ac;
                sharicomplient := cur_var.u06_sharia_complient;
            -- MNM
            END IF;
        END IF;

        sexchange := cur_var.u06_exchange;
        comm_cat := cur_var.u06_commision_type;
        comm_group_id := cur_var.u06_commision_group_id;
        allow_channels := cur_var.u06_allow_all_channals;
        parms :=
               parms
            || '!'
            || cur_var.u06_exchange
            || ','
            || cur_var.u06_routing_ac_type
            || ','
            || NVL (routing_ac, '*')
            || ','
            || cur_var.u06_commision_type
            || ','
            || NVL (cur_var.u06_executing_broker_ref, '*')
            || ','
            || NVL (cur_var.u06_commision_amt_small, 0)
            || ','
            || NVL (cur_var.u06_is_custodian_account, 0)
            || ','
            || NVL (cur_var.u06_commision_share, 0)
            || ','
            || NVL (cur_var.u06_txn_fee, 0)
            || ','
            || NVL (sharicomplient, 0)
            || ','
            || NVL (cur_var.u06_cond_orders, 0)
            || ','
            || NVL (cur_var.u06_symbol_cond, 0)
            || ','
            || NVL (cur_var.u06_daydiscount, 0)
            || ','
            || NVL (comm_group_id, 0)
            || ','
            || NVL (cur_var.u06_exec_broker_inst, 0)
            || ','
            || NVL (cur_var.u06_exec_broker_rebate, 0)
            || ','
            || NVL (cur_var.u06_custodian_inst_id, 0)
            || ','
            || NVL (cur_var.u06_allow_all_channals, 1)
            || ','
            || NVL (rebate_group_id, 0)
            || ','
            || NVL (cur_var.u06_commsion_discount_group_id, 0)
            || ','
            || NVL (cur_var.u06_commision_group_id, 0)
            || ','
            || NVL (cur_var.u06_comm_disc_appl_per, 0)
            || ','
            || NVL (cur_var.u06_stock_transaction_restrict, 0)
            || ','
            || NVL (cur_var.u06_stock_transaction_rtc_narr, '*')
            || ','
            || NVL (cur_var.u06_stock_transfer_disabled, 0)
            || ','
            || NVL (cur_var.u06_stock_transfer_disabled_na, '*')
            || ','
            || NVL (cur_var.u06_trading_restriction, 0)
            || ','
            || NVL (cur_var.u06_trading_restriction_narr, '*')
            || ','
            || NVL (cur_var.u06_pledge_restriction, 0)
            || ','
            || NVL (cur_var.u06_pledge_restriction_narr, '*')
            || ','
            || NVL (cur_var.u06_depositor_ac, '*')
            || ','
            --|| NVL (cur_var.u06_secondary_commission_group, 0)
            || '1'
            || ','
            || NVL (cur_var.u06_comm_group_type, 1)
            || ','
            || NVL (cur_var.u06_exec_broker_sid, '*')
            || ','
            || NVL (cur_var.u06_trading_enabled, 0)
            || ','
            || NVL (cur_var.u06_invester_ac_no, 0)
            || ','
            || NVL (cur_var.u06_concentration_group, 0)
            || ','
            || NVL (cur_var.u06_min_order_qty, 0)
            || ','
            || NVL (cur_var.u06_sharia_complient_grp, 0)
            || ','
            || NVL (cur_var.u06_market_segment, 0);



        IF (allow_channels = 0)
        THEN
            OPEN restricted_channel_cursor (sexchange);

            FETCH restricted_channel_cursor INTO restricted_channel_var;

            WHILE restricted_channel_cursor%FOUND
            LOOP
                parms :=
                       parms
                    || ','
                    || NVL (restricted_channel_var.u20_order_channel, 0)
                    || '^'
                    || NVL (restricted_channel_var.u20_type, 3);

                FETCH restricted_channel_cursor INTO restricted_channel_var;
            END LOOP;

            CLOSE restricted_channel_cursor;
        END IF;


        OPEN symbol_restriction_cursor (customer_id, sec_acc_id, sexchange);

        FETCH symbol_restriction_cursor INTO symbol_restriction_var;

        WHILE symbol_restriction_cursor%FOUND
        LOOP
            parms :=
                   parms
                || ','
                || symbol_restriction_var.u08_symbol
                || '~'
                || symbol_restriction_var.u08_type;

            FETCH symbol_restriction_cursor INTO symbol_restriction_var;
        END LOOP;

        CLOSE symbol_restriction_cursor;

        OPEN instrument_restrict_cursor (customer_id, sec_acc_id, sexchange);

        FETCH instrument_restrict_cursor INTO instrument_restrict_var;

        WHILE instrument_restrict_cursor%FOUND
        LOOP
            parms :=
                   parms
                || ','
                || instrument_restrict_var.u10_instrument_type_id
                || '#'
                || instrument_restrict_var.u10_exchange;

            FETCH instrument_restrict_cursor INTO instrument_restrict_var;
        END LOOP;


        CLOSE instrument_restrict_cursor;

        OPEN sector_restrict_cursor (customer_id, sec_acc_id, sexchange);

        FETCH sector_restrict_cursor INTO sector_restrict_var;

        WHILE sector_restrict_cursor%FOUND
        LOOP
            parms :=
                   parms
                || ','
                || sector_restrict_var.u09_sectorcode
                || '<'
                || sector_restrict_var.u09_exchangecode;

            FETCH sector_restrict_cursor INTO sector_restrict_var;
        END LOOP;

        CLOSE sector_restrict_cursor;



        IF (rebate_group_id > 0)
        THEN
            OPEN cust_rebate_cursor;

            FETCH cust_rebate_cursor INTO rebate_var;

            WHILE cust_rebate_cursor%FOUND
            LOOP
                parms :=
                       parms
                    || ','
                    || NVL (rebate_var.m82_from_value, 0)
                    || '$'
                    || NVL (rebate_var.m82_to_value, 0)
                    || '$'
                    || NVL (rebate_var.m82_percentage, 0)
                    || '$'
                    || NVL (rebate_var.m82_flat_amount, 0)
                    || '$'
                    || NVL (rebate_var.m82_currency, 'SAR');

                FETCH cust_rebate_cursor INTO rebate_var;
            END LOOP;

            CLOSE cust_rebate_cursor;
        END IF;

        FETCH rout_acc_cursor INTO cur_var;
    END LOOP;

    CLOSE rout_acc_cursor;
END;
/