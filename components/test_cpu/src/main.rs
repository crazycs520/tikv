use futures::executor::block_on;
use resource_metering::{cpu::FutureExt, ResourceMeteringTag, TagInfos};
use std::task::{Context, Poll};
use std::time::Duration;

async fn handle_cop_request(need_row_count: u64) {
    println!("handle_cop_request");
}

fn main() {
    exec_future(0, 10);
}

fn exec_future(region_id: u64, row_count: u64) {
    let future = handle_cop_request(row_count);
    let tag = ResourceMeteringTag::new(TagInfos::new(region_id, vec![1, 2, 3]));
    let future = future.in_resource_metering_tag(tag);
    block_on(future);
}
