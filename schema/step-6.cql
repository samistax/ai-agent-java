CREATE TABLE IF NOT EXISTS datastax_ai_agent.network_traffic (
    unit_id int,
    dtime timestamp,
    sk_tx_bytes bigint,
    sk_rx_bytes bigint,
    cust_wired_tx_bytes bigint,
    cust_wired_rx_bytes bigint,
    cust_wifi_tx_bytes bigint,
    cust_wifi_rx_bytes bigint,
    PRIMARY KEY (unit_id, dtime)
);
