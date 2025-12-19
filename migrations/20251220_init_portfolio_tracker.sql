-- ============================================================
-- 1. Core Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS data (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  trade_date DATE NOT NULL,
  stock TEXT NOT NULL,
  action TEXT NOT NULL,
  quantity NUMERIC,
  price NUMERIC,
  trade_value NUMERIC,
  brokerage NUMERIC,
  ratio TEXT,
  mark TEXT,
  account_name TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lookup_lists (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  co_name TEXT NOT NULL,
  balance NUMERIC DEFAULT 0,
  net_investment NUMERIC DEFAULT 0,
  avg_cost NUMERIC DEFAULT 0,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE (user_id, co_name)
);

CREATE TABLE IF NOT EXISTS lookup_lists_gain (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  co_name TEXT NOT NULL,
  cumulative_gain NUMERIC DEFAULT 0,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE (user_id, co_name)
);

CREATE TABLE IF NOT EXISTS share_trading (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  company_name TEXT NOT NULL,
  trade_date DATE NOT NULL,
  shares NUMERIC,
  buy_sell TEXT NOT NULL,
  description TEXT,
  account_name TEXT,
  price NUMERIC,
  brokerage NUMERIC,
  total_amount NUMERIC,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portfolio (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  co_name TEXT NOT NULL,
  date_of DATE NOT NULL,
  shares NUMERIC NOT NULL,
  price NUMERIC,
  brokerage NUMERIC,
  amount NUMERIC,
  description TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS investment_acct (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  co_name TEXT NOT NULL,
  trade_date DATE NOT NULL,
  buy NUMERIC,
  sell NUMERIC,
  balance NUMERIC,
  rate NUMERIC,
  amount NUMERIC,
  brokerage NUMERIC,
  sell_amount NUMERIC,
  buy_amount NUMERIC,
  total_amount NUMERIC,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS capital_gain (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  company_name TEXT NOT NULL,
  buy_date DATE,
  buy_qty NUMERIC,
  buy_price NUMERIC,
  buy_brokerage NUMERIC,
  buy_amount NUMERIC,
  sell_date DATE,
  sell_qty NUMERIC,
  sell_rate NUMERIC,
  sell_brokerage NUMERIC,
  sell_amount NUMERIC,
  short_term_gain NUMERIC,
  long_term_gain NUMERIC,
  total_gain NUMERIC,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gain_loss (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  company_name TEXT NOT NULL,
  trade_date DATE NOT NULL,
  gain_loss NUMERIC,
  cumulative NUMERIC,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mthly_gain (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  company_name TEXT NOT NULL,
  buy_date DATE,
  buy_qty NUMERIC,
  buy_price NUMERIC,
  buy_brokerage NUMERIC,
  buy_total NUMERIC,
  sell_date DATE,
  sell_qty NUMERIC,
  sell_rate NUMERIC,
  sell_brokerage NUMERIC,
  sell_total NUMERIC,
  total_gain NUMERIC,
  cumulative_gain NUMERIC,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS last_buy_sell (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  company_name TEXT NOT NULL,
  shares NUMERIC,
  avg_cost NUMERIC,
  investment NUMERIC,
  sale_receipt NUMERIC,
  last_buy1 DATE, last_sell1 DATE,
  last_buy2 DATE, last_sell2 DATE,
  last_buy3 DATE, last_sell3 DATE,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE (user_id, company_name)
);

CREATE TABLE IF NOT EXISTS last_buy_sell_total (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,
  company_name TEXT NOT NULL,
  shares NUMERIC,
  avg_cost NUMERIC,
  investment NUMERIC,
  sale_receipt NUMERIC,
  created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- 2. Helper Functions
-- ============================================================

CREATE OR REPLACE FUNCTION round_num(val NUMERIC, decimals INT)
RETURNS NUMERIC AS $$
BEGIN
  RETURN ROUND(val::NUMERIC, decimals);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION upsert_lookup_buy(uid UUID, company TEXT, qty NUMERIC, price NUMERIC, brokerage NUMERIC)
RETURNS VOID AS $$
BEGIN
  INSERT INTO lookup_lists (user_id, co_name, balance, net_investment, avg_cost)
  VALUES (uid, company, qty, (qty * price + brokerage), ROUND(((qty * price + brokerage) / NULLIF(qty,0)), 2))
  ON CONFLICT (user_id, co_name)
  DO UPDATE SET
    balance = lookup_lists.balance + EXCLUDED.balance,
    net_investment = lookup_lists.net_investment + EXCLUDED.net_investment,
    avg_cost = ROUND((lookup_lists.net_investment + (qty * price + brokerage)) / NULLIF(lookup_lists.balance + qty, 0), 2);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_lookup_sell(uid UUID, company TEXT, qty NUMERIC)
RETURNS VOID AS $$
DECLARE
  cur_balance NUMERIC;
  cur_net NUMERIC;
  cur_avg NUMERIC;
  new_balance NUMERIC;
  cogs NUMERIC;
  new_net NUMERIC;
BEGIN
  SELECT balance, net_investment, avg_cost INTO cur_balance, cur_net, cur_avg
  FROM lookup_lists WHERE user_id = uid AND co_name = company;

  IF cur_balance IS NULL THEN
    RAISE EXCEPTION 'Company % not found', company;
  END IF;
  IF cur_balance - qty < 0 THEN
    RAISE EXCEPTION 'Sell qty exceeds balance for %', company;
  END IF;

  cogs := cur_avg * qty;
  new_balance := cur_balance - qty;
  new_net := cur_net - cogs;

  UPDATE lookup_lists
  SET balance = new_balance,
      net_investment = new_net,
      avg_cost = CASE WHEN new_balance > 0 THEN ROUND(new_net / new_balance, 2) ELSE 0 END
  WHERE user_id = uid AND co_name = company;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION upsert_lookup_gain(uid UUID, company TEXT, gain NUMERIC)
RETURNS VOID AS $$
BEGIN
  INSERT INTO lookup_lists_gain (user_id, co_name, cumulative_gain)
  VALUES (uid, company, gain)
  ON CONFLICT (user_id, co_name)
  DO UPDATE SET cumulative_gain = lookup_lists_gain.cumulative_gain + EXCLUDED.cumulative_gain;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_last_buy_sell(
  uid UUID,
  company TEXT,
  trade_date DATE,
  balance NUMERIC,
  amt NUMERIC,
  is_sell BOOLEAN DEFAULT FALSE,
  sell_date DATE DEFAULT NULL,
  current_gain NUMERIC DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
  INSERT INTO last_buy_sell (user_id, company_name, shares, avg_cost, investment, sale_receipt)
  VALUES (uid, company, balance, ROUND(-amt / NULLIF(balance,0),2), -amt, current_gain)
  ON CONFLICT (user_id, company_name)
  DO UPDATE SET
    shares = EXCLUDED.shares,
    avg_cost = EXCLUDED.avg_cost,
    investment = EXCLUDED.investment,
    sale_receipt = EXCLUDED.sale_receipt;

  IF is_sell THEN
    UPDATE last_buy_sell SET last_sell1 = sell_date WHERE user_id = uid AND company_name = company;
  ELSE
    UPDATE last_buy_sell SET last_buy1 = trade_date WHERE user_id = uid AND company_name = company;
  END IF;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- 3. Transaction Processors
-- ============================================================

-- BUY
CREATE OR REPLACE FUNCTION process_buy_transaction(
  uid UUID,
  company TEXT,
  tdate DATE,
  qty NUMERIC,
  price NUMERIC,
  brokerage NUMERIC,
  ratio TEXT DEFAULT NULL,
  account TEXT DEFAULT 'Default'
) RETURNS VOID AS $$
DECLARE
  total_cost NUMERIC := qty * price + COALESCE(brokerage,0);
  cur_balance NUMERIC;
  cur_net NUMERIC;
BEGIN
  INSERT INTO portfolio (user_id, co_name, date_of, shares, price, brokerage, amount, description)
  VALUES (uid, company, tdate, qty, price, brokerage, total_cost, ratio);

  PERFORM upsert_lookup_buy(uid, company, qty, price, COALESCE(brokerage,0));

  SELECT balance, net_investment INTO cur_balance, cur_net
  FROM lookup_lists WHERE user_id = uid AND co_name = company;

  INSERT INTO investment_acct (user_id, co_name, trade_date, buy, balance, rate, amount, brokerage, buy_amount, total_amount)
  VALUES (uid, company, tdate, qty, cur_balance, price, qty * price, brokerage, total_cost, total_cost);

  INSERT INTO share_trading (user_id, company_name, trade_date, shares, buy_sell, description, account_name, price, brokerage, total_amount)
  VALUES (uid, company, tdate, qty, 'Buy', ratio, account, price, brokerage, total_cost);

  PERFORM update_last_buy_sell(uid, company, tdate, cur_balance, -cur_net, FALSE, NULL, NULL);
END;
$$ LANGUAGE plpgsql;

-- SELL
CREATE OR REPLACE FUNCTION process_sell_transaction(
  uid UUID,
  company TEXT,
  tdate DATE,
  qty NUMERIC,
  price NUMERIC,
  brokerage NUMERIC,
  account TEXT DEFAULT 'Default'
) RETURNS VOID AS $$
DECLARE
  remaining NUMERIC := qty;
  total_gain NUMERIC := 0;
  lot RECORD;
  sell_brok_per_unit NUMERIC := COALESCE(brokerage,0) / NULLIF(qty,0);
  applied_qty NUMERIC;
  buy_amt NUMERIC;
  slice_gain NUMERIC;
  cur_balance NUMERIC;
  cur_net NUMERIC;
BEGIN
  SELECT balance INTO cur_balance FROM lookup_lists WHERE user_id = uid AND co_name = company;
  IF cur_balance IS NULL OR cur_balance < qty THEN
    RAISE EXCEPTION 'Sell qty exceeds balance for %', company;
  END IF;

  FOR lot IN
    SELECT id, date_of AS buy_date, shares AS buy_qty, price AS buy_price, brokerage AS buy_brokerage, amount AS buy_amount
    FROM portfolio
    WHERE user_id = uid AND co_name = company
    ORDER BY date_of ASC
  LOOP
    EXIT WHEN remaining <= 0;
    applied_qty := LEAST(remaining, lot.buy_qty);
    buy_amt := ROUND((lot.buy_amount / NULLIF(lot.buy_qty,0)) * applied_qty, 2);
    slice_gain := ROUND((applied_qty * price - (sell_brok_per_unit * applied_qty)) - buy_amt, 2);

    INSERT INTO capital_gain (
      user_id, company_name, buy_date, buy_qty, buy_price, buy_brokerage, buy_amount,
      sell_date, sell_qty, sell_rate, sell_brokerage, sell_amount,
      short_term_gain, long_term_gain, total_gain
    ) VALUES (
      uid, company, lot.buy_date, lot.buy_qty, lot.buy_price, lot.buy_brokerage, lot.buy_amount,
      tdate, applied_qty, price, ROUND(sell_brok_per_unit * applied_qty, 2),
      ROUND(applied_qty * price - ROUND(sell_brok_per_unit * applied_qty, 2), 2),
      CASE WHEN (tdate - lot.buy_date) <= 365 THEN slice_gain ELSE NULL END,
      CASE WHEN (tdate - lot.buy_date) > 365 THEN slice_gain ELSE NULL END,
      slice_gain
    );

    total_gain := total_gain + slice_gain;

    IF lot.buy_qty = applied_qty THEN
      DELETE FROM portfolio WHERE id = lot.id;
    ELSE
      UPDATE portfolio
      SET shares = lot.buy_qty - applied_qty,
          brokerage = ROUND(lot.buy_brokerage * ((lot.buy_qty - applied_qty) / lot.buy_qty), 2),
          amount = ROUND(lot.buy_amount * ((lot.buy_qty - applied_qty) / lot.buy_qty), 2)
      WHERE id = lot.id;
    END IF;

    remaining := remaining - applied_qty;
  END LOOP;

  PERFORM update_lookup_sell(uid, company, qty);

  SELECT balance, net_investment INTO cur_balance, cur_net
  FROM lookup_lists WHERE user_id = uid AND co_name = company;

  INSERT INTO investment_acct (user_id, co_name, trade_date, sell, balance, rate, amount, brokerage, sell_amount, total_amount)
  VALUES (uid, company, tdate, qty, cur_balance, price, qty * price, brokerage, qty * price - brokerage, qty * price - brokerage);

  INSERT INTO share_trading (user_id, company_name, trade_date, shares, buy_sell, account_name, price, brokerage, total_amount)
  VALUES (uid, company, tdate, qty, 'Sell', account, price, brokerage, qty * price - brokerage);

  INSERT INTO gain_loss (user_id, company_name, trade_date, gain_loss, cumulative)
  VALUES (uid, company, tdate, total_gain,
    (SELECT COALESCE(SUM(gain_loss),0) FROM gain_loss WHERE user_id = uid AND company_name = company) + total_gain);

  INSERT INTO mthly_gain (user_id, company_name, sell_date, sell_qty, sell_rate, sell_brokerage, sell_total, total_gain, cumulative_gain)
  VALUES (uid, company, tdate, qty, price, brokerage, qty * price - brokerage, total_gain,
    (SELECT COALESCE(SUM(total_gain),0) FROM mthly_gain WHERE user_id = uid) + total_gain);

  PERFORM upsert_lookup_gain(uid, company, total_gain);

  PERFORM update_last_buy_sell(uid, company, tdate, cur_balance, -cur_net, TRUE, tdate, total_gain);
END;
$$ LANGUAGE plpgsql;

-- SPLIT
CREATE OR REPLACE FUNCTION process_split_transaction(
  uid UUID,
  company TEXT,
  tdate DATE,
  new_qty NUMERIC,
  ratio TEXT
) RETURNS VOID AS $$
DECLARE
  r INT := 1;
  org_qty NUMERIC;
  add_qty NUMERIC;
  cur_balance NUMERIC;
  cur_net NUMERIC;
BEGIN
  BEGIN r := split_part(ratio, ':', 2)::INT; EXCEPTION WHEN OTHERS THEN r := 1; END;
  org_qty := new_qty / NULLIF(r,0);
  add_qty := new_qty - org_qty;

  UPDATE lookup_lists
  SET balance = balance + add_qty
  WHERE user_id = uid AND co_name = company;

  SELECT balance, net_investment INTO cur_balance, cur_net
  FROM lookup_lists WHERE user_id = uid AND co_name = company;

  INSERT INTO investment_acct (user_id, co_name, trade_date, buy, balance, total_amount)
  VALUES (uid, company, tdate, add_qty, cur_balance, -cur_net);

  PERFORM update_last_buy_sell(uid, company, tdate, cur_balance, -cur_net, FALSE, NULL, NULL);
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- 4. Orchestration (continued)
-- ============================================================

CREATE OR REPLACE FUNCTION process_transactions(uid UUID, max_rows INT DEFAULT NULL)
RETURNS TABLE (processed_count INT) AS $$
DECLARE
  rec RECORD;
  cnt INT := 0;
BEGIN
  FOR rec IN
    SELECT * FROM data
    WHERE user_id = uid AND (mark IS NULL OR mark <> '#')
    ORDER BY trade_date, id
    LIMIT COALESCE(max_rows, NULL)
  LOOP
    IF rec.action IN ('Buy','Bonus','OpgBal') THEN
      PERFORM process_buy_transaction(uid, rec.stock, rec.trade_date, rec.quantity, rec.price, rec.brokerage, rec.ratio, COALESCE(rec.account_name,'Default'));
    ELSIF rec.action = 'Sell' THEN
      PERFORM process_sell_transaction(uid, rec.stock, rec.trade_date, rec.quantity, rec.price, rec.brokerage, COALESCE(rec.account_name,'Default'));
    ELSIF rec.action IN ('Split','Amalgam') THEN
      PERFORM process_split_transaction(uid, rec.stock, rec.trade_date, rec.quantity, COALESCE(rec.ratio,'1:1'));
    ELSE
      -- ignore unknown actions gracefully
    END IF;

    UPDATE data SET mark = '#' WHERE id = rec.id;
    cnt := cnt + 1;
  END LOOP;

  RETURN QUERY SELECT cnt;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 5. Views
-- ============================================================

CREATE OR REPLACE VIEW portfolio_sort_v AS
SELECT
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY co_name, date_of) AS srl_no,
  co_name,
  date_of,
  shares,
  price,
  brokerage,
  (shares * price + COALESCE(brokerage,0)) AS amount,
  SUM(shares) OVER (PARTITION BY user_id, co_name ORDER BY date_of) AS balance,
  SUM((shares * price + COALESCE(brokerage,0))) OVER (PARTITION BY user_id, co_name ORDER BY date_of) AS total
FROM portfolio
WHERE user_id = auth.uid()
ORDER BY co_name, date_of;

CREATE OR REPLACE VIEW mthly_gain_summary_v AS
SELECT
  DATE_TRUNC('month', sell_date) AS month_year,
  SUM(total_gain) AS monthly_total_gain,
  SUM(SUM(total_gain)) OVER (PARTITION BY user_id ORDER BY DATE_TRUNC('month', sell_date)) AS cumulative_total_gain
FROM mthly_gain
WHERE user_id = auth.uid()
GROUP BY user_id, DATE_TRUNC('month', sell_date)
ORDER BY month_year;

CREATE OR REPLACE VIEW gain_loss_summary_v AS
SELECT
  company_name,
  trade_date,
  gain_loss,
  SUM(gain_loss) OVER (PARTITION BY user_id, UPPER(company_name) ORDER BY trade_date) AS cumulative_gain
FROM gain_loss
WHERE user_id = auth.uid()
ORDER BY UPPER(company_name), trade_date;

CREATE OR REPLACE VIEW capital_gainnew_v AS
WITH base AS (
  SELECT
    user_id,
    company_name,
    buy_date,
    buy_qty,
    buy_price,
    buy_brokerage,
    buy_amount,
    sell_date,
    sell_qty,
    sell_rate,
    sell_brokerage,
    sell_amount,
    short_term_gain,
    long_term_gain,
    (COALESCE(short_term_gain,0) + COALESCE(long_term_gain,0)) AS total_gain
  FROM capital_gain
  WHERE user_id = auth.uid()
)
SELECT * FROM base
UNION ALL
SELECT
  user_id,
  company_name,
  NULL, SUM(buy_qty), SUM(buy_price), SUM(buy_brokerage), SUM(buy_amount),
  NULL, SUM(sell_qty), NULL, SUM(sell_brokerage), SUM(sell_amount),
  SUM(COALESCE(short_term_gain,0)), SUM(COALESCE(long_term_gain,0)), SUM(total_gain)
FROM base
GROUP BY user_id, company_name
ORDER BY company_name, buy_date NULLS LAST;

CREATE OR REPLACE VIEW capital_gain_grand_total_v AS
SELECT SUM(total_gain) AS grand_total_gain
FROM capital_gain
WHERE user_id = auth.uid();

-- ============================================================
-- 6. Reset and Rollover
-- ============================================================

CREATE OR REPLACE FUNCTION clear_results(uid UUID)
RETURNS VOID AS $$
BEGIN
  DELETE FROM share_trading WHERE user_id = uid;
  DELETE FROM portfolio WHERE user_id = uid;
  DELETE FROM investment_acct WHERE user_id = uid;
  DELETE FROM capital_gain WHERE user_id = uid;
  DELETE FROM capital_gainnew WHERE user_id = uid;
  DELETE FROM gain_loss WHERE user_id = uid;
  DELETE FROM mthly_gain WHERE user_id = uid;
  DELETE FROM last_buy_sell WHERE user_id = uid;
  DELETE FROM last_buy_sell_total WHERE user_id = uid;
  DELETE FROM lookup_lists_gain WHERE user_id = uid;
  UPDATE data SET mark = NULL WHERE user_id = uid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION clear_all(uid UUID)
RETURNS VOID AS $$
BEGIN
  PERFORM clear_results(uid);
  DELETE FROM data WHERE user_id = uid;
  DELETE FROM lookup_lists WHERE user_id = uid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION rollover_financial_year(uid UUID, new_fy TEXT)
RETURNS VOID AS $$
BEGIN
  IF new_fy !~ '^[0-9]{4}-[0-9]{2}$' THEN
    RAISE EXCEPTION 'Invalid FY format; expected YYYY-YY';
  END IF;

  PERFORM clear_results(uid);

  INSERT INTO data (user_id, trade_date, stock, action, quantity, price, trade_value, brokerage, ratio, account_name, mark)
  SELECT
    uid,
    CURRENT_DATE,
    co_name,
    'OpgBal',
    balance,
    avg_cost,
    balance * avg_cost,
    0,
    NULL,
    'Opening Balance',
    NULL
  FROM lookup_lists
  WHERE user_id = uid AND balance > 0;
END;
$$ LANGUAGE plpgsql;


