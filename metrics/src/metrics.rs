

use prometheus::{Histogram, IntCounter};
use prometheus::{register_histogram, register_int_counter};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref AVG_PRICE_PER_MINUTE: Histogram = register_histogram!(
        "avg_price_per_minute",
        "Average price per minute",
    )
    .expect("Can't create a metric");
}

lazy_static! {
    pub static ref MISSING_VALUES_COUNT: IntCounter = register_int_counter!(
        "missing_values_count",
        "Number of missing values"
    )
    .expect("Can't create a metric");
}
