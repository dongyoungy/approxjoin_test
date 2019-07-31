import sample_gen as sg

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

# sg.create_cent_sample_pair_for_all_from_impala(
#     "cp-9",
#     21050,
#     "synthetic_10m",
#     "normal_1",
#     "col1",
#     "col2",
#     "col2",
#     "synthetic_10m",
#     "normal_2",
#     "col1",
#     "synthetic_10m_all2",
#     1,
#     False,
# )

sg.create_cent_stratified_sample_pair_from_impala(
    "cp-18",
    21050,
    "synthetic_10m",
    "normal_powerlaw_1",
    "col1",
    "col2",
    "col3",
    "synthetic_10m",
    "normal_2",
    "col1",
    "st_test3",
    "count",
    1500000,
    50000,
    1,
    False,
)

