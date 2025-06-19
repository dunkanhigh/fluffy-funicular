CREATE TABLE covid
(
date Date,
county String,
state String,
cases Int64,
deaths Int32,
population Int32,
ISO FixedString(5),
density_per_sq_mi Int32
)
ENGINE = MergeTree()
ORDER BY tuple(date, state)
PARTITION BY state