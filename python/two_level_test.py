import two_level_sample_gen as tl

tl.create_two_level_sample_pair_from_impala(
    "cp-4",
    21050,
    "synthetic_small",
    "uniform_1_small",
    "col1",
    "synthetic_small",
    "uniform_2_small",
    "col1",
    "synthetic_10m_2lv_test",
    1,
)

