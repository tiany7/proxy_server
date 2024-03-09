
fn main() {
    tonic_build::compile_protos("proto/market.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
    tonic_build::compile_protos("proto/trade.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}