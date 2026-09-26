use asap_physical_operators::{AggregateCore, Statistic};
use asap_physical_operators::planner::post_asap::{ExactKind, ExactParams, SummaryFamilyType};
use asap_physical_operators::summary_kernels::exact::ExactAccumulator;
use std::{alloc::{GlobalAlloc, Layout, System}, collections::HashMap, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
struct Allocations;
static COUNT: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Allocations {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 { COUNT.fetch_add(1, Ordering::Relaxed); System.alloc(layout) }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) { System.dealloc(ptr, layout) }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 { COUNT.fetch_add(1, Ordering::Relaxed); System.realloc(ptr, layout, size) }
}
#[global_allocator] static ALLOCATIONS: Allocations = Allocations;
fn main() {
    for (kind, param, stat) in [(ExactKind::Rate, ExactParams::Rate, Statistic::Rate), (ExactKind::Sum, ExactParams::Sum, Statistic::Sum)] {
        let mut states: Vec<Arc<dyn AggregateCore>> = vec![];
        for pane in 0..12 {
            let mut state = ExactAccumulator::new(SummaryFamilyType::ExactAggregate(kind.clone(), param.clone()), false).unwrap();
            for sample in 0..4 { state.update(None, ((pane * 4 + sample) % 17) as f64, pane * 5000 + sample * 1000); }
            states.push(Arc::new(state));
        }
        let range = || HashMap::from([("range_start_ms".into(), 0_u64.to_string()), ("range_end_ms".into(), 60000_u64.to_string())]);
        let baseline = || (0..8).map(|_| {
            asap_physical_operators::stored_state::readout::exact_readout_optional(states.iter().cloned(), stat, &None, &range()).unwrap().unwrap()
        }).collect::<Vec<_>>();
        let shared = || {
            let params = if stat==Statistic::Rate { range() } else { HashMap::new() };
            (0..8).map(|_| asap_physical_operators::stored_state::readout::exact_readout_optional(states.iter().cloned(), stat, &None, &params).unwrap().unwrap()).collect::<Vec<_>>()
        };
        assert_eq!(baseline(), shared());
        COUNT.store(0, Ordering::Relaxed); std::hint::black_box(baseline()); let old=COUNT.load(Ordering::Relaxed);
        COUNT.store(0, Ordering::Relaxed); std::hint::black_box(shared()); let new=COUNT.load(Ordering::Relaxed);
        println!("8-group {:?}: equal results, allocations {} -> {}", stat, old,new);
    }
}
