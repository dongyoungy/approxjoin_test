import sample_gen as sg
import evaluate_sample as es

# es.run_synthetic_stratified_ours(
#     "cp-10",
#     21050,
#     "synthetic_10m",
#     "synthetic_10m_strat_new",
#     "normal_powerlaw2_1",
#     "normal_2",
#     "count",
#     0.03,
#     0.01,
#     1000,
#     10,
#     1,
# )

for agg in ["count", "sum", "avg"]:
    es.run_synthetic_stratified_ours(
        "cp-10",
        21050,
        "synthetic_10m",
        "synthetic_10m_strat_new",
        "normal_powerlaw2_1",
        "normal_2",
        agg,
        0.03,
        0.01,
        1000,
        10,
        100,
    )

# sg.create_join_key_stat_table("cp-10", 21050, "synthetic_10m", "normal_2", "col1")

# sg.create_group_stat_table(
#     "cp-10", 21050, "synthetic_10m", "normal_powerlaw2_1", "col1", "col2", "col3", 0
# )

# sg.create_cent_sample_pair_for_all_from_impala(
#     "cp-9",
#     21050,
#     "instacart",
#     "orders",
#     "order_id",
#     "days_since_prior",
#     "order_dow",
#     "instacart",
#     "order_products",
#     "order_id",
#     "instacart_all2",
#     1,
#     False,
# )

# sg.create_cent_sample_pair_from_impala(
#     "cp-9",
#     21050,
#     "synthetic_10m",
#     "normal_1",
#     "col1",
#     "col2",
#     "synthetic_10m",
#     "normal_2",
#     "col1",
#     None,
#     "time_measure",
#     "sum",
#     1,
#     False,
#     True,
# )

# instacart

# sg.create_cent_sample_pair_from_impala(
#     "cp-9",
#     21050,
#     "instacart",
#     "orders",
#     "order_id",
#     "order_hour_of_day",
#     "instacart",
#     "order_products",
#     "order_id",
#     None,
#     "time_measure",
#     "sum",
#     1,
#     False,
#     True,
# )

# sg.create_dec_sample_pair_from_impala(
#     "cp-9",
#     21050,
#     "instacart",
#     "orders",
#     "order_id",
#     "order_hour_of_day",
#     "instacart",
#     "order_products",
#     "order_id",
#     None,
#     "dec_time_measure",
#     "sum",
#     1,
#     False,
#     True,
# )

# tpch
# sg.create_cent_sample_pair_from_impala(
#     "cp-9",
#     21050,
#     "tpch100g_parquet",
#     "lineitem",
#     "l_orderkey",
#     "l_quantity",
#     "tpch100g_parquet",
#     "orders",
#     "o_orderkey",
#     None,
#     "time_measure",
#     "sum",
#     1,
#     False,
#     True,
# )

# sg.create_dec_sample_pair_from_impala(
#     "cp-9",
#     21050,
#     "tpch100g_parquet",
#     "lineitem",
#     "l_orderkey",
#     "l_quantity",
#     "tpch100g_parquet",
#     "orders",
#     "o_orderkey",
#     None,
#     "dec_time_measure",
#     "sum",
#     1,
#     False,
#     True,
# )


# sg.create_cent_stratified_sample_pair_from_impala(
#     "cp-18",
#     21050,
#     "synthetic_10m",
#     "normal_powerlaw2_1",
#     "col1",
#     "col2",
#     "col3",
#     "synthetic_10m",
#     "normal_2",
#     "col1",
#     "old_st_test2",
#     "count",
#     10000,
#     10,
#     0.03,
#     0.01,
#     1,
#     False,
# )

# sg.create_cent_stratified_sample_pair_from_impala_new(
#     "cp-18",
#     21050,
#     "synthetic_10m",
#     "normal_powerlaw2_1",
#     "col1",
#     "col2",
#     "col3",
#     "synthetic_10m",
#     "normal_2",
#     "col1",
#     "new_st_test2",
#     "count",
#     10000,
#     100000,
#     0.03,
#     0.01,
#     1,
#     False,
# )

